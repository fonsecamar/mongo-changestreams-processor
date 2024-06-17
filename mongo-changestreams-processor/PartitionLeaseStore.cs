using mongo_changestreams_processor.Entities;
using MongoDB.Driver;

namespace mongo_changestreams_processor
{
    internal class PartitionLeaseStore
    {
        private IMongoCollection<PartitionLease> _leaseCollection;
        private string _instanceId;
        private LeaseOptions _options;

        internal PartitionLeaseStore(IMongoCollection<PartitionLease> leaseCollection, string instanceId, LeaseOptions options)
        {
            _leaseCollection = leaseCollection;
            _instanceId = instanceId;
            _options = options;
        }

        internal async Task<Dictionary<string, PartitionLease>> GetAllLeasesAsync(string processorFullName)
        {
            var result = await _leaseCollection.FindAsync(Builders<PartitionLease>.Filter.Eq(p => p.processor, processorFullName), new FindOptions<PartitionLease>() { BatchSize = 100 });
            return result.ToEnumerable().ToDictionary(p => p._id);
        }

        internal async Task<bool> CreateNewLeaseAsync(PartitionLease lease)
        {
            try
            {
                await _leaseCollection.InsertOneAsync(lease);
                return true;
            }
            catch (MongoWriteException ex)
            {
                if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    return false;
                else
                    throw;
            }
        }

        internal async Task RequestBalanceIntentAsync(PartitionLease lease)
        {
            var builder = Builders<PartitionLease>.Filter;
            var filter = builder.Eq(p => p.processor, lease.processor);
            filter &= builder.Eq(p => p._id, lease._id);
            filter &= builder.Or(builder.Eq(p => p.balanceRequest, string.Empty), builder.Eq(p => p.balanceRequest, _instanceId));

            var update = Builders<PartitionLease>.Update.Set(p => p.balanceRequest, _instanceId);

            try
            {
                await _leaseCollection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true });
            }
            catch (MongoCommandException ex)
            {
                if (ex.Code == 11000)
                    return;
                else
                    throw;
            }
            catch(MongoWriteException ex)
            {
                if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    return;
                else
                    throw;
            }
        }

        internal async Task<PartitionLease?> AcquireLeaseAsync(PartitionLease lease)
        {
            var builder = Builders<PartitionLease>.Filter;
            var filter = builder.Eq(p => p.processor, lease.processor);
            filter &= builder.Eq(p => p._id, lease._id);
            filter &= builder.Eq(p => p.owner, string.Empty);
            filter &= builder.Or(builder.Eq(p => p.balanceRequest, string.Empty), builder.Eq(p => p.balanceRequest, _instanceId));

            lease.owner = _instanceId;
            lease.balanceRequest = _options.AllowBalance ? string.Empty : _instanceId;

            var update = Builders<PartitionLease>.Update
                .Set(p => p.owner, lease.owner)
                .Set(p => p.timeStamp, DateTime.UtcNow)
                .Set(p => p.balanceRequest, lease.balanceRequest);

            PartitionLease? result = null;

            try
            {
                result = await _leaseCollection.FindOneAndUpdateAsync(filter, update, new FindOneAndUpdateOptions<PartitionLease> { IsUpsert = true, ReturnDocument = ReturnDocument.After });

                if (result.owner == _instanceId)
                {
                    return result;
                }
                else
                    return null;
            }
            catch (MongoCommandException ex)
            {
                if (ex.Code == 11000)
                    return null;
                else
                    throw;
            }
            catch (MongoWriteException ex)
            {
                if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    return null;
                else
                    throw;
            }
        }

        internal async Task ReleaseLeaseAsync(PartitionLease lease)
        {
            var builder = Builders<PartitionLease>.Filter;
            var filter = builder.Eq(p => p.processor, lease.processor);
            filter &= builder.Eq(p => p._id, lease._id);
            filter &= builder.Eq(p => p.owner, lease.owner);

            lease.owner = string.Empty;
            lease.balanceRequest = lease.balanceRequest == _instanceId ? string.Empty : lease.balanceRequest;

            var update = Builders<PartitionLease>.Update
                .Set(p => p.owner, lease.owner)
                .Set(p => p.timeStamp, DateTime.UtcNow)
                .Set(p => p.balanceRequest, lease.balanceRequest);

            await _leaseCollection.UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true });
        }

        internal async Task StoreResumeToken(PartitionLease lease)
        {
            // Build filter for upsert token for a particular thread
            var builder = Builders<PartitionLease>.Filter;
            var filter = builder.Eq(p => p.processor, lease.processor);
            filter &= builder.Eq(p => p._id, lease._id);
            filter &= builder.Eq(p => p.owner, _instanceId);

            var update = Builders<PartitionLease>.Update
                .Set(p => p.token, lease.token)
                .Set(p => p.timeStamp, DateTime.UtcNow);

            var projection = Builders<PartitionLease>.Projection.Include(p => p.balanceRequest);//.Include(p => p.owner);

            // Upsert the token
            var result = await _leaseCollection.FindOneAndUpdateAsync(filter, Builders<PartitionLease>.Update.Combine(update), new FindOneAndUpdateOptions<PartitionLease> { IsUpsert = true, ReturnDocument = ReturnDocument.After, Projection = projection });

            lease.balanceRequest = result.balanceRequest;

            lease.ResetLeaseControl();
        }
    }
}
