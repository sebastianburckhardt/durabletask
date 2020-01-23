using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace DurableTask.EventSourced.AzureChannels
{
    internal abstract class AzureTableHub<TMessage> : BatchWorker<(TMessage,TableOperation)> where TMessage: class
    {
        private readonly string taskHubName;
        private readonly string hostId;
        private readonly string baseFilterCondition;

        protected Dictionary<string, long> LastReceived { get; private set; } = new Dictionary<string, long>();

        public CloudTable Table { get; }

        private const string TableName = "Channels";

         public AzureTableHub(
             CancellationToken token, 
             string taskHubId, 
             string hostId, 
             CloudTableClient tableClient)
            : base(token)
        {
            this.taskHubName = taskHubId;
            this.hostId = hostId;
            this.Table = tableClient.GetTableReference(TableName);

            var taskHubCondition = TableQuery.GenerateFilterCondition(
                "PartitionKey",
                QueryComparisons.Equal,
                taskHubName);

            var targetsThisHostCondition = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("RowKey",
                    QueryComparisons.GreaterThanOrEqual,
                    $"{this.hostId}-"),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey",
                    QueryComparisons.LessThanOrEqual,
                    $"{this.hostId}-~"));

            this.baseFilterCondition = TableQuery.CombineFilters(taskHubCondition, TableOperators.And, targetsThisHostCondition);
        }

        public Task CreateTableIfNotExistAsync()
        {
            return Table.CreateIfNotExistsAsync();
        }

        public void Send(TMessage msg, string source, string destination, long offset, byte[] content, string debug)
        {
            Submit((msg, TableOperation.InsertOrReplace(new ContentEntity(taskHubName, source, destination, offset, content, debug))));
        }

        public void DeleteRange(IEnumerable<ContentEntity> entities)
        {
            this.SubmitRange(entities.Select(entity => ((TMessage)null, TableOperation.Delete(entity))));
        }

        protected abstract void HandleSuccessfulSend(TMessage msg);
        protected abstract bool HandleFailedSend(TMessage msg, Exception exception);

        public async Task<List<ContentEntity>> Receive()
        {
            var query = new TableQuery<ContentEntity>().Where(FilterCondition().ToString());

            TableContinuationToken continuationToken = null;
            List<ContentEntity> result = new List<ContentEntity>();

            do // retry if no results are returned
            {
                do // continue query while there is a continuation token
                {
                    var nextbatch = await this.Table.ExecuteQuerySegmentedAsync<ContentEntity>(query, continuationToken, null, null, this.cancellationToken);

                    foreach (var entity in nextbatch)
                    {
                        long lastReceived = 0;
                        this.LastReceived.TryGetValue(entity.Source, out lastReceived);
                        if (entity.Offset <= lastReceived)
                        {
                            continue; // duplicate
                        }
                        else
                        {
                            result.Add(entity);
                            this.LastReceived[entity.Source] = entity.Offset;
                        }
                    }

                    continuationToken = nextbatch.ContinuationToken;
                }
                while (continuationToken != null && !this.cancellationToken.IsCancellationRequested);

                if (result.Count > 0)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100), this.cancellationToken);
            }
            while (!this.cancellationToken.IsCancellationRequested);

            return result;
        }

        private string FilterCondition()
        {
            string disjunctionOfExcludedRanges = null;

            foreach (var kvp in LastReceived)
            {
                if (kvp.Value == -1)
                {
                    continue;
                }

                string source = kvp.Key;
                long lastReceived = kvp.Value;
                string destination = this.hostId;

                var excludedRange = TableQuery.CombineFilters(
                   TableQuery.GenerateFilterCondition("RowKey",
                       QueryComparisons.GreaterThanOrEqual,
                       $"{destination}-{source}-"),
                   TableOperators.And,
                   TableQuery.GenerateFilterCondition("RowKey",
                       QueryComparisons.LessThanOrEqual,
                       $"{destination}-{source}-{lastReceived:X16}"));


                if (disjunctionOfExcludedRanges == null)
                {
                    disjunctionOfExcludedRanges = excludedRange;
                }
                else
                {
                    disjunctionOfExcludedRanges = TableQuery.CombineFilters(disjunctionOfExcludedRanges, TableOperators.Or, excludedRange);
                }
            }

            if (disjunctionOfExcludedRanges != null)
            {
                return $"({this.baseFilterCondition}) and (not ({disjunctionOfExcludedRanges}))";
            }
            else
            {
                return this.baseFilterCondition;
            }
        }

        public void Delete(ContentEntity entity)
        {
            Submit((null, TableOperation.Delete(entity)));
        }

        public void Delete(IEnumerable<ContentEntity> entities)
        {
            SubmitRange(entities.Select(entity => ((TMessage)null, TableOperation.Delete(entity))));
        }

        protected override async ValueTask ProcessAsync(IList<(TMessage,TableOperation)> batch)
        {
            TableBatchOperation tableBatch = new TableBatchOperation();
            List<TMessage> messages = new List<TMessage>();

            foreach ((TMessage msg, TableOperation operation) in batch)
            {
                tableBatch.Add(operation);
                messages.Add(msg);

                if (tableBatch.Count == 100)
                {
                    await ExecuteBatch(tableBatch, messages);
                }
            }


            if (tableBatch.Count > 0)
            {
                await ExecuteBatch(tableBatch, messages);
            }
        }

        private async Task ExecuteBatch(TableBatchOperation tableBatch, List<TMessage> messages)
        {
            try
            {               
                await Table.ExecuteBatchAsync(tableBatch);
                foreach(var msg in messages)
                {
                    if (msg != null)
                    {
                        this.HandleSuccessfulSend(msg);
                    }
                }
            }
            catch(Exception e)
            {
                for (int i = 0; i < messages.Count; i++)
                {
                    var msg = messages[i];
                    if (msg != null)
                    {
                        bool requeue = this.HandleFailedSend(msg, e);
                        if (requeue)
                        {
                            this.Submit((msg, tableBatch[i]));
                        }
                    }
                }
            }

            tableBatch.Clear();
            messages.Clear();
        }

        public class ContentEntity : TableEntity
        {
            public ContentEntity()
            {
            }

            public ContentEntity(string taskHubName, string source, string destination, long offset, byte[] content, string debug)
                : base(taskHubName, $"{destination}-{source}-{offset:X16}")
            {
                Source = source;
                Content = content;
                Debug = debug;
                ETag = "*"; // no conditions when inserting, replace existing
            }

            public string TaskHubName => PartitionKey;
            public long Offset => Convert.ToInt64(RowKey.Substring(RowKey.Length - 16), 16);

            public string Source { get; set; }
            public byte[] Content { get; set; }
            public string Debug { get; set; }
        }
    }
}
