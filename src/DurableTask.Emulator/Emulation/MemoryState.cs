using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class MemoryState : IPartitionState
    {
        public ClocksState Clocks { get; private set; } = new ClocksState();

        public OutboxState Outbox { get; private set; } = new OutboxState();

        public TimersState Timers { get; private set; } = new TimersState();

        public ActivitiesState Activities { get; private set; } = new ActivitiesState();

        public SessionsState Sessions { get; private set; } = new SessionsState();

        private Dictionary<string, InstanceState> instances = new Dictionary<string, InstanceState>();

        private LocalOrchestrationService localPartition;

        public InstanceState GetInstance(string instanceId)
        {
            if (! instances.TryGetValue(instanceId, out var instance))
            {
                this.instances[instanceId] = instance = new InstanceState();
                instance.Restore(this.localPartition);
            }
            return instance;
        }

        public Task<long> Restore(LocalOrchestrationService localPartition)
        {
            this.localPartition = localPartition;

            long nextToProcess = 0;

            foreach(var trackedObject in this.GetTrackedObjects())
            {
                long lastProcessed = trackedObject.Restore(localPartition);

                if (lastProcessed > nextToProcess)
                {
                    nextToProcess = lastProcessed;
                }
            }

            return Task.FromResult(nextToProcess);
        }

        private IEnumerable<TrackedObject> GetTrackedObjects()
        {
            yield return Clocks;
            yield return Outbox;
            yield return Timers;
            yield return Activities;
            yield return Sessions;

            foreach(var kvp in instances)
            {
                yield return kvp.Value;
            }
        }

        // reuse these collection objects between updates (note that updates are never concurrent by design)
        List<TrackedObject> scope = new List<TrackedObject>();
        List<TrackedObject> apply = new List<TrackedObject>();

        public Task UpdateAsync(PartitionEvent evt)
        {
            var target = evt.Scope(this);
            target.Process(evt, scope, apply);
            scope.Clear();
            apply.Clear();
            return Task.FromResult<object>(null);
        }

        public Task<TResult> ReadAsync<TResult>(Func<TResult> read)
        {
            if (!(read.Target is TrackedObject))
            {
                throw new ArgumentException("Target must be a tracked object.", nameof(read));
            }
            lock (read.Target)
            {
                return Task.FromResult(read());
            }
        }

        public Task<TResult> ReadAsync<TArgument1, TResult>(Func<TArgument1, TResult> read, TArgument1 argument)
        {
            if (!(read.Target is TrackedObject))
            {
                throw new ArgumentException("Target must be a tracked object.", nameof(read));
            }
            lock (read.Target)
            {
                return Task.FromResult(read(argument));
            }
        }
    }
}
