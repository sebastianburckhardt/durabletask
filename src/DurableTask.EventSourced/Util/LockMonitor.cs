using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace DurableTask.EventSourced
{
    internal class LockMonitor
    {
        public const int TopN = 3;

        private int[] topAcquire;
        private int[] topRelease;
        private string[] topAcquireReports;
        private string[] topReleaseReports;
        private int acquireThreshold;
        private int releaseThreshold;

        public static LockMonitor Instance { get; } = new LockMonitor();

        private LockMonitor()
        {
            this.Reset();
        }

        public void ReportAcquire(string name, long ticks)
        {
            if (this.FindSpotInTop((int)ticks / 1000, topAcquire, ref acquireThreshold, out int index))
            {
                topAcquireReports[index] = $"{name}.waited={(double)ticks/TimeSpan.TicksPerMillisecond:F1}";
            }
        }

        public void ReportRelease(string name, long ticks)
        {
            if (this.FindSpotInTop((int)ticks / 1000, topRelease, ref releaseThreshold, out int index))
            {
                topReleaseReports[index] = $"{name}.held={(double)ticks / TimeSpan.TicksPerMillisecond:F1}";
            }
        }

        public void Reset()
        {
            this.acquireThreshold = int.MaxValue;
            this.releaseThreshold = int.MaxValue;
            this.topAcquireReports = new string[TopN];
            this.topReleaseReports = new string[TopN];
            this.topAcquire = new int[TopN];
            this.topRelease = new int[TopN];
            this.acquireThreshold = 1;
            this.releaseThreshold = 1;
        }

        public string Report()
        {
            return string.Join(" ", Enumerate());

            IEnumerable<string> Enumerate()
            {
                foreach (string x in topAcquireReports)
                {
                    if (x != null)
                    {
                        yield return x;
                    }
                }
                foreach (string x in topReleaseReports)
                {
                    if (x != null)
                    {
                        yield return x;
                    }
                }
            }
        }

        private bool FindSpotInTop(int result, int[] top, ref int threshold, out int index)
        {
            index = 0;
            if (result > threshold)
            {
                int min = result;
                for (int i = 0; i < TopN; i++)
                {
                    while (true)
                    {
                        int topi = top[i];
                        if (topi < result)
                        {
                            var read = Interlocked.CompareExchange(ref top[i], result, topi);
                            if (read == topi)
                            {
                                index = i;
                                return true;
                            }
                            else
                            {
                                continue;
                            }
                        }
                        else if (min > topi)
                        {
                            min = topi;
                        }
                        break;
                    }
                }
                if (min > threshold)
                {
                    threshold = min;
                }
            }
            return false;
        }
    }
}
