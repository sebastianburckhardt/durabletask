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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Emulated
{
    [DataContract]
    internal class EmulatedStorage : BatchWorker<PartitionEvent>, Storage.IPartitionState
    {
        [DataMember]
        public DedupState Dedup { get; private set; }

        [DataMember]
        public ReassemblyState Reassembly { get; private set; }

        [DataMember]
        public ClientsState Clients { get; private set; }

        [DataMember]
        public OutboxState Outbox { get; private set; }

        [DataMember]
        public TimersState Timers { get; private set; }

        [DataMember]
        public ActivitiesState Activities { get; private set; }

        [DataMember]
        public RecoveryState Recovery { get; private set; }

        [DataMember]
        public SessionsState Sessions { get; private set; }

        [DataMember]
        internal long CommitPosition { get; private set; }


        private ConcurrentDictionary<string, InstanceState> instances;
        private ConcurrentDictionary<string, HistoryState> histories;

        private Partition partition;

        private Func<string, InstanceState> instanceFactory;
        private Func<string, HistoryState> historyFactory;

        public EmulatedStorage()
        {
            this.Dedup = new DedupState();
            this.Clients = new ClientsState();
            this.Reassembly = new ReassemblyState();
            this.Outbox = new OutboxState();
            this.Timers = new TimersState();
            this.Activities = new ActivitiesState();
            this.Recovery = new RecoveryState();
            this.Sessions = new SessionsState();
            this.instances = new ConcurrentDictionary<string, InstanceState>();
            this.histories = new ConcurrentDictionary<string, HistoryState>();
            this.instanceFactory = MakeInstance;
            this.historyFactory = MakeHistory;
        }

        public InstanceState GetInstance(string instanceId)
        {
            return instances.GetOrAdd(instanceId, this.instanceFactory);
        }

        public HistoryState GetHistory(string instanceId)
        {
            return histories.GetOrAdd(instanceId, this.historyFactory);
        }

        private InstanceState MakeInstance(string instanceId)
        {
            var instance = new InstanceState();
            instance.InstanceId = instanceId;
            instance.Restore(this.partition);
            return instance;
        }

        private HistoryState MakeHistory(string instanceId)
        {
            var history = new HistoryState();
            history.InstanceId = instanceId;
            history.Restore(this.partition);
            return history;
        }

        public Task RestoreAsync(Partition partition)
        {
            this.partition = partition;

            // first, connect objects to partition and restart all in-flight tasks
            this.Dedup.Restore(partition);
            this.Clients.Restore(partition);
            this.Reassembly.Restore(partition);
            this.Outbox.Restore(partition);
            this.Timers.Restore(partition);
            this.Activities.Restore(partition);
            this.Recovery.Restore(partition);
            this.Sessions.Restore(partition);
            foreach(var instance in instances.Values)
            {
                instance.Restore(partition);
            }
            foreach(var history in histories.Values)
            {
                history.Restore(partition);
            }

            // then, finish any potentially incomplete commit batches
            if (this.Recovery.Pending != null)
            {
                this.Process(this.Recovery.Pending);
            }

            return Task.CompletedTask;
        }

        public Task ShutdownAsync()
        {
            return Task.Delay(10);
        }

        public void Commit(PartitionEvent evt)
        {
            this.Submit(evt);
        }

        protected override Task Process(List<PartitionEvent> batch)
        {
            Recovery.Pending = batch;

            this.ProcessBatch(batch, Recovery.LastProcessed + 1);

            Recovery.LastProcessed = Recovery.LastProcessed + batch.Count;
            Recovery.Pending = null;

            return Task.CompletedTask;
        }

        private void ProcessBatch(List<PartitionEvent> batch, long nextCommitPosition)
        {
            for (int i = 0; i < batch.Count; i++)
            {
                var partitionEvent = batch[i];
                partitionEvent.CommitPosition = nextCommitPosition + i;
                partition.TraceProcess(partitionEvent);
                var target = partitionEvent.StartProcessingOnObject(this);
                target.ProcessRecursively(partitionEvent, tracker);
                tracker.Clear();
            }
        }

        // reuse these collection objects between updates (note that updates are never concurrent by design)
        TrackedObject.EffectTracker tracker = new TrackedObject.EffectTracker();

        public Task<TResult> ReadAsync<TResult>(Func<TResult> read)
        {
            if (!(read.Target is TrackedObject to))
            {
                throw new ArgumentException("Target must be a tracked object.", nameof(read));
            }
            lock (to.AccessLock)
            {
                return Task.FromResult(read());
            }
        }

        public Task<TResult> ReadAsync<TArgument1, TResult>(Func<TArgument1, TResult> read, TArgument1 argument)
        {
            if (!(read.Target is TrackedObject to))
            {
                throw new ArgumentException("Target must be a tracked object.", nameof(read));
            }
            lock (to.AccessLock)
            {
                return Task.FromResult(read(argument));
            }
        }

        public void Update(TrackedObject target, PartitionEvent evt)
        {
            dynamic dynamicTarget = target;
            dynamic dynamicPartitionEvent = evt;
            dynamicTarget.Apply(dynamicPartitionEvent);

            target.LastProcessed = evt.CommitPosition;
        }
    }
}