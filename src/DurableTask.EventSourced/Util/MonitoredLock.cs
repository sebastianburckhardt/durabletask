using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace DurableTask.EventSourced
{
    // serves as a standard lock but with monitoring built in
    internal class MonitoredLock
    {
        private readonly string name;
        private readonly Stopwatch stopWatch;

        public MonitoredLock(string name) 
        {
            this.name = name;
            this.stopWatch = new Stopwatch();
            this.stopWatch.Start();
        }

        public AcquisitionToken Lock()
        {
            long startTime = stopWatch.ElapsedTicks;
            System.Threading.Monitor.Enter(this);
            long acquiredTime = stopWatch.ElapsedTicks;
            LockMonitor.Instance.ReportAcquire(this.name, acquiredTime - startTime);
            return new AcquisitionToken()
            {
                MonitoredLock = this,
                AcquiredTime = acquiredTime
            };
        }

        internal struct AcquisitionToken : IDisposable
        {
            public MonitoredLock MonitoredLock;
            public long AcquiredTime;

            public void Dispose()
            {
                LockMonitor.Instance.ReportRelease(this.MonitoredLock.name, this.MonitoredLock.stopWatch.ElapsedTicks - this.AcquiredTime);
                System.Threading.Monitor.Exit(this.MonitoredLock);
            }
        }
    }
}
