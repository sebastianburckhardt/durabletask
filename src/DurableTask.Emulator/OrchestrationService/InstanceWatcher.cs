using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;

namespace DurableTask.Emulator
{
    internal class StateWatcher
    {
        private readonly Dictionary<string, InstanceWatcher> watchers = new Dictionary<string, InstanceWatcher>();

        public delegate void StatusChangedHandler(InstanceWatcher watcher, OrchestrationStatus status);
        public delegate void SessionLoadedHandler(InstanceWatcher watcher, string InstanceId);
        public delegate void SessionLockedHandler(InstanceWatcher watcher);
        public delegate void SessionAbandonedHandler(InstanceWatcher watcher);
        public delegate void SessionCompletedHandler(InstanceWatcher watcher);

        public event StatusChangedHandler StatusChanged;
        public event SessionLoadedHandler SessionLoaded;
        public event SessionLockedHandler SessionLocked;
        public event SessionAbandonedHandler SessionAbandoned;
        public event SessionCompletedHandler SessionCompleted;

        public class InstanceWatcher
        {
            public event StatusChangedHandler StatusChanged;
            public event SessionLoadedHandler SessionLoaded;
            public event SessionLockedHandler SessionLocked;
            public event SessionAbandonedHandler SessionAbandoned;
            public event SessionCompletedHandler SessionCompleted;
        }

        
        public async Task<OrchestrationStatus> GetInstanceStatus()
        {
            if (watchers.TryGetValue())
        }
    }
}
