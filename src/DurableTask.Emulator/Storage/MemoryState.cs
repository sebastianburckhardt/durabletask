﻿using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.Emulator
{
    internal class MemoryState : IState
    {
        public ClocksState Clocks { get; set; }

        public OutboxState Outbox { get; set; }

        public TimersState Timers { get; set; }

        public ActivitiesState Activities { get; set; }

        public SessionsState Sessions { get; set; }

        private Dictionary<string, InstanceState> Instances { get; set; }

        public InstanceState GetInstance(string instanceId)
        {
            if (! Instances.TryGetValue(instanceId, out var instance))
            {
                this.Instances[instanceId] = instance = new InstanceState();
            }
            return instance;
        }

        public Task RestoreAsync(LocalPartition localPartition)
        {
            foreach(var trackedObject in this.GetTrackedObjects())
            {
                trackedObject.Restore(localPartition);
            }
            return Task.FromResult(0);
        }

        private IEnumerable<TrackedObject> GetTrackedObjects()
        {
            yield return Clocks;
            yield return Outbox;
            yield return Timers;
            yield return Activities;
            yield return Sessions;

            foreach(var kvp in Instances)
            {
                yield return kvp.Value;
            }
        }

        // reuse these collection objects between updates (note that updates are never concurrent by design)
        List<TrackedObject> scope = new List<TrackedObject>();
        List<TrackedObject> apply = new List<TrackedObject>();

        public Task UpdateAsync(ProcessorEvent evt)
        {
            var target = evt.Scope(this);
            target.Process(evt, scope, apply);
            scope.Clear();
            apply.Clear();
            return Task.FromResult(0);
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
    }
}
