﻿//  ----------------------------------------------------------------------------------
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
using System.Collections.Generic;

namespace DurableTask.EventSourced.Faster
{
    internal class PsfBlobCheckpointManager : ICheckpointManager
    {
        private readonly BlobManager blobManager;
        private readonly int groupOrdinal;

        internal PsfBlobCheckpointManager(BlobManager blobMan, int groupOrd)
        {
            this.blobManager = blobMan;
            this.groupOrdinal = groupOrd;
        }

        void ICheckpointManager.InitializeIndexCheckpoint(Guid indexToken)
        { } // there is no need to create empty directories in a blob container

        void ICheckpointManager.InitializeLogCheckpoint(Guid logToken)
        { } // there is no need to create empty directories in a blob container

        void ICheckpointManager.CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
            => this.blobManager.CommitIndexCheckpoint(indexToken, commitMetadata, this.groupOrdinal);

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
            => this.blobManager.CommitLogCheckpoint(logToken, commitMetadata, this.groupOrdinal);

        byte[] ICheckpointManager.GetIndexCheckpointMetadata(Guid indexToken)
            => this.blobManager.GetIndexCommitMetadata(indexToken, this.groupOrdinal);

        byte[] ICheckpointManager.GetLogCheckpointMetadata(Guid logToken)
            => this.blobManager.GetLogCommitMetadata(logToken, this.groupOrdinal);

        /// <summary>
        /// Get list of index checkpoint tokens, in order of usage preference
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Guid> GetIndexCheckpointTokens()
        {
            return this.blobManager.GetLatestCheckpoint(out Guid indexToken, out _, this.groupOrdinal)
                ? new[] { indexToken } : Array.Empty<Guid>();
        }

        /// <summary>
        /// Get list of log checkpoint tokens, in order of usage preference
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Guid> GetLogCheckpointTokens()
        {
            return this.blobManager.GetLatestCheckpoint(out _, out Guid logToken, this.groupOrdinal)
                ? new[] { logToken } : Array.Empty<Guid>();
        }

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
            => this.blobManager.GetIndexDevice(indexToken, this.groupOrdinal);

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
            => this.blobManager.GetSnapshotLogDevice(token, this.groupOrdinal);

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
            => this.blobManager.GetSnapshotObjectLogDevice(token, this.groupOrdinal);

        public void PurgeAll() { /* TODO */ }

        public void Dispose() { }
    }
}
