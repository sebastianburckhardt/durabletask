using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DurableTask.Core;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// Schema for instance queries
    /// </summary>
    internal static class InstanceQueries
    {
        /// <summary>
        /// Specification of an instance query
        /// </summary>
        public interface IQuerySpec
        {
            /// <summary>
            /// The subset of runtime statuses to return, or null if all
            /// </summary>
            public OrchestrationStatus[] RuntimeStatus { get; }

            /// <summary>
            /// The lowest creation time to return, or null if no lower bound
            /// </summary>
            public DateTime? CreatedTimeFrom { get; }

            /// <summary>
            /// The latest creation time to return, or null if no upper bound
            /// </summary>
            public DateTime? CreatedTimeTo { get; }

            /// <summary>
            /// A prefix of the instance ids to return, or null if no specific prefix
            /// </summary>
            public string InstanceIdPrefix { get; }
        }
 
        public static bool HasRuntimeStatus(this IQuerySpec t) => !(t.RuntimeStatus is null) && t.RuntimeStatus.Length > 0;

        public static bool IsSet(this IQuerySpec t) => t.HasRuntimeStatus() || !string.IsNullOrWhiteSpace(t.InstanceIdPrefix)
                                    || !(t.CreatedTimeFrom is null) || !(t.CreatedTimeTo is null);

        public static bool Matches(this IQuerySpec t, OrchestrationState targetState)
                => (!t.HasRuntimeStatus() || t.RuntimeStatus.Contains(targetState.OrchestrationStatus))
                     && (string.IsNullOrWhiteSpace(t.InstanceIdPrefix) || targetState.OrchestrationInstance.InstanceId.StartsWith(t.InstanceIdPrefix))
                     && (t.CreatedTimeFrom is null || targetState.CreatedTime >= t.CreatedTimeFrom)
                     && (t.CreatedTimeTo is null || targetState.CreatedTime <= t.CreatedTimeTo);
    }
}
