using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Blob.Protocol;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    internal static class BlobUtils
    {
        public static async Task<bool> ForceDeleteAsync(CloudBlob blob)
        {
            try
            {
                await blob.DeleteAsync();
                return true;
            }
            catch (StorageException e) when (BlobDoesNotExist(e))
            {
                return false;
            }
            catch (StorageException e) when (CannotDeleteBlobWithLease(e))
            {
                try
                {
                    await blob.BreakLeaseAsync(TimeSpan.Zero);
                }
                catch
                {
                    // we ignore exceptions in the lease breaking since there could be races
                }

                // retry the delete
                try
                {
                    await blob.DeleteAsync();
                    return true;
                }
                catch (StorageException ex) when (BlobDoesNotExist(ex))
                {
                    return false;
                }
            }
        }

        // Lease error codes are documented at https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob

        public static bool LeaseConflictOrExpired(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 409) || (e.RequestInformation.HttpStatusCode == 412);
        }

        public static bool LeaseConflict(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 409);
        }

        public static bool LeaseExpired(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 412);
        }

        public static bool CannotDeleteBlobWithLease(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 412);
        }

        public static bool BlobDoesNotExist(StorageException e)
        {
            var information = e.RequestInformation.ExtendedErrorInformation;
            return (e.RequestInformation.HttpStatusCode == 404) && (information.ErrorCode.Equals(BlobErrorCodeStrings.BlobNotFound));
        }
    }
}
