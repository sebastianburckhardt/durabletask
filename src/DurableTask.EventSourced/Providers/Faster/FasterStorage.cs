//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http.Headers;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterStorage : Storage.IPartitionState
    {
        private Partition partition;
        private CommitWorker commitWorker;
        private FasterKV store;
        private CancellationToken token;

        public Task RestoreAsync(Partition partition)
        {
            this.partition = partition;

            var directory = Path.Combine(Path.GetTempPath(), "faster");
            var logFileName = Path.Combine(directory, $"part{partition.PartitionId:D2}.log");
            var objectLogFileName = Path.Combine(directory, $"part{partition.PartitionId:D2}.obj.log");
            var checkpoints = Path.Combine(directory, $"part{partition.PartitionId:D2}.checkpoints");
            IDevice log = Devices.CreateLogDevice(logFileName.ToString());
            IDevice objlog = Devices.CreateLogDevice(objectLogFileName.ToString());

            store = new FasterKV(partition, log, objlog, checkpoints);

            this.token = partition.PartitionShutdownToken;
            this.commitWorker = new CommitWorker(store, partition, token);

            return Task.CompletedTask;
        }

        public Task WaitForTerminationAsync()
        {
            return this.commitWorker.JoinAsync();
        }

        public Task<TResult> ReadAsync<TObject, TResult>(TrackedObjectKey key, Func<TObject, TResult> read) where TObject : TrackedObject
        {
            var task = new Task<TResult>(() =>
            {
                try
                {
                    var target = store.GetOrCreate(key);

                    TResult result;

                        result = read((TObject)target);

                    return result;
                }
                catch (Exception e)
                {
                    // TODO

                    throw e;
                }
            });

            commitWorker.Submit(task);
            return task;
        }

        public void Submit(PartitionEvent evt)
        {
            commitWorker.Submit(evt);
        }

        public void SubmitRange(IEnumerable<PartitionEvent> evts)
        {
            commitWorker.SubmitRange(evts);
        }

    }
}