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

        byte[] ICheckpointManager.GetIndexCommitMetadata(Guid indexToken)
            => this.blobManager.GetIndexCommitMetadata(indexToken, this.groupOrdinal);

        byte[] ICheckpointManager.GetLogCommitMetadata(Guid logToken)
            => this.blobManager.GetLogCommitMetadata(logToken, this.groupOrdinal);

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
            => this.blobManager.GetIndexDevice(indexToken, this.groupOrdinal);

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
            => this.blobManager.GetSnapshotLogDevice(token, this.groupOrdinal);

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
            => this.blobManager.GetSnapshotObjectLogDevice(token, this.groupOrdinal);

        bool ICheckpointManager.GetLatestCheckpoint(out Guid indexToken, out Guid logToken)
            => this.blobManager.GetLatestCheckpoint(out indexToken, out logToken, this.groupOrdinal);
    }
}
