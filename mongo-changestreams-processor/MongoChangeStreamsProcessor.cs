using mongo_changestreams_processor.Entities;
using MongoDB.Bson;
using MongoDB.Driver;

namespace mongo_changestreams_processor
{
    public class MongoChangeStreamsProcessor
    {
        private readonly string _instanceId = Guid.NewGuid().ToString();
        private MongoChangeStreamsProcessorBuilder _builder;

        private MongoClient _mongoClient;
        private IMongoDatabase _database;
        private IMongoCollection<BsonDocument> _collection;
        private IMongoCollection<PartitionLease> _leaseCollection;

        private string _processorName;
        private string _databaseName;
        private string _collectionName;

        private int _batchSize;
        private int _leaseRenewalInterval;
        private int _leaseExpirationInterval;

        private bool _isRunning = false;
        private Task _monitorPartitionsTask = Task.CompletedTask;
        private List<Task> _runningPartitions = new();
        private CancellationTokenSource _cancellation = new();

        public delegate Task ChangesHandler(IEnumerable<BsonDocument> changes, CancellationToken cancellationToken);

        internal MongoChangeStreamsProcessor(MongoChangeStreamsProcessorBuilder builder, MongoClient mongoClient)
        {
            _builder = builder;
            _mongoClient = mongoClient;

            _processorName = _builder._processorName;
            _databaseName = _builder._databaseName;
            _collectionName = _builder._collectionName;

            _batchSize = _builder._batchSize;
            _leaseRenewalInterval = _builder._leaseRenewalInterval;
            _leaseExpirationInterval = _builder._leaseExpirationInterval;

            // Initialize the processor
            _database = _mongoClient.GetDatabase(_databaseName);
            _collection = _database.GetCollection<BsonDocument>(_collectionName);
            _leaseCollection = (_builder._leaseClient ?? _mongoClient).GetDatabase(_builder._leaseDatabaseName ?? _databaseName).GetCollection<PartitionLease>(_builder._leaseCollectionName ?? "leases");

            if (_database == null || _collection == null || _leaseCollection == null)
                throw new Exception("Database, Collection or Lease Collection not found");
        }

        public async Task StartAsync(ChangesHandler onChangesDelegate)
        {
            if (_isRunning)
                return;

            // Start the processor
            var acquiredPartitions = await AcquireLeaseAsync(_cancellation.Token);
            //_monitorPartitionsTask = MonitorPartitionsAsync(cancellation);

            if (acquiredPartitions.Count == 0)
                throw new Exception("Cannot acquire lease");

            foreach (var partition in acquiredPartitions)
            {
                // Start the processor for each partition
                _runningPartitions.Add(ProcessPartitionAsync(partition.Value, onChangesDelegate, _cancellation.Token));
            }

            _isRunning = true;
        }

        public async Task StopAsync()
        {
            if (!_isRunning)
                return;

            // Stop the processor
            _cancellation.Cancel();

            await Task.WhenAll(_runningPartitions);

            _runningPartitions.Clear();
            _isRunning = false;
        }

        internal async Task<Dictionary<int, PartitionLease>> AcquireLeaseAsync(CancellationToken cancellation)
        {
            Dictionary<int, PartitionLease> partitions;
            bool leaseAcquired = false;

            do
            {
                var filter = Builders<PartitionLease>.Filter.Eq(p => p.processor, $"{_processorName}-{_mongoClient.Settings.Server.Host}-{_database.DatabaseNamespace}-{_collectionName}");
                var leases = await _leaseCollection.FindAsync<PartitionLease>(filter, cancellationToken: cancellation);

                if (await leases.MoveNextAsync() && leases.Current.Count() > 0)
                {
                    // Use found tokens to resume
                    var tempPartitions = leases.Current.ToDictionary(x => x.partitionNumber, x => x);
                    // Get partitions that have no owners or lease has expired. Filter out partitions that are not owned by this instance.
                    partitions = tempPartitions.Where(x => x.Value.owner == _instanceId || x.Value.owner == string.Empty || (x.Value.IsLeaseExpired(_leaseExpirationInterval))).ToDictionary();
                }
                else
                {
                    var startDate = _builder._startFromBeginning ? DateTime.MinValue : _builder._startTime;
                    // If no leases found, get tokens from database
                    partitions = await GetPartitionTokensAsync(startDate, cancellation);
                }

                if (partitions.Count() == 0)
                {
                    if(_builder._printDebugLogs)
                        await Console.Out.WriteLineAsync("DEBUG: Lease not acquired. Sleeping...");
                    
                    await Task.Delay(_leaseRenewalInterval, cancellation);
                    continue;
                }

                foreach (var partition in partitions)
                {
                    await StoreResumeToken(partition.Value);
                }

                if (_builder._printDebugLogs)
                    await Console.Out.WriteLineAsync("DEBUG: Lease acquired!");

                leaseAcquired = true;

            } while (!leaseAcquired);

            return partitions;
        }

        internal async Task<Dictionary<int, PartitionLease>> GetPartitionTokensAsync(DateTime initialTimestamp, CancellationToken cancellation)
        {
            Dictionary<int, PartitionLease> partitions = new();

            // If this is initial run (no leases found), get tokens from database and consider the timestamp parameter
            var diff = initialTimestamp.ToUniversalTime() - DateTime.UnixEpoch;

            // Build database command to get Stream Tokens for each physical partition for a particular collection
            var streamCommand = new BsonDocumentCommand<BsonDocument>(new BsonDocument
                {
                    { "customAction", "GetChangeStreamTokens" },
                    { "collection", _collectionName},
                    { "startAtOperationTime", new BsonTimestamp((int)diff.TotalSeconds, 0)},
                });

            var streams = await _database.RunCommandAsync(streamCommand, null, cancellation);

            // Use tokens returned from database
            int counter = 1;
            foreach (var p in streams["resumeAfterTokens"].AsBsonArray)
            {
                var partition = new PartitionLease() 
                {
                    _id = $"{_processorName}-{counter}-{_mongoClient.Settings.Server.Host}-{_database.DatabaseNamespace}-{_collectionName}",
                    processor = $"{_processorName}-{_mongoClient.Settings.Server.Host}-{_database.DatabaseNamespace}-{_collectionName}",
                    owner = _instanceId,
                    partitionNumber = counter, 
                    token = p.AsBsonDocument, 
                    balanceControl = !_builder._allowBalance ? "Single" : "None" 
                };
                partitions.Add(counter, partition);
                counter++;
            }

            return partitions;
        }

        internal async Task ProcessPartitionAsync(PartitionLease lease, ChangesHandler onChangesDelegate, CancellationToken cancellation)
        {
            bool leaseReleased = false;
            bool leaseRenewalRequired = false;

            // Create a pipeline
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
                .Match(change => change.OperationType == ChangeStreamOperationType.Insert || change.OperationType == ChangeStreamOperationType.Update || change.OperationType == ChangeStreamOperationType.Replace)
                .AppendStage<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>, BsonDocument>(
                    "{ $project: { '_id': 1, 'fullDocument': 1, 'ns': 1, 'documentKey': 1 }}");

            // Create options
            var options = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                ResumeAfter = lease.token, // Provide the resume tokens to watcher
                BatchSize = _batchSize, //Define the batch size
            };

            try
            {
                // Watch the collection
                using (var cursor = await _collection.WatchAsync(pipeline, options, cancellation))
                {
                    // Loop through the changes
                    while (await cursor.MoveNextAsync() && !cancellation.IsCancellationRequested)
                    {
                        leaseRenewalRequired = false;
                        try
                        {
                            if (cursor.Current != null && cursor.Current.Count() > 0)
                            {
                                // Get resume token for this batch
                                lease.token = cursor.GetResumeToken();

                                // Handle received list of documents
                                await onChangesDelegate(cursor.Current, cancellation);

                                leaseRenewalRequired = true;
                            }
                            else
                            {
                                leaseRenewalRequired = lease.IsLeaseRenewalRequired(_leaseRenewalInterval);
                            }

                            leaseReleased = cancellation.IsCancellationRequested;
                            // After successfully handling documents or reached lease renewal interval, store new resume token or renew the lease
                            if (leaseReleased || leaseRenewalRequired)
                                await StoreResumeToken(lease, leaseReleased);
                        }
                        catch (Exception ex)
                        {
                            await Console.Out.WriteLineAsync(ex.Message);
                        }
                    }
                }

                if (!leaseReleased)
                    await StoreResumeToken(lease, true);
            }
            catch(Exception ex)
            {
                await Console.Out.WriteLineAsync(ex.Message);
            }
        }

        internal async Task StoreResumeToken(PartitionLease lease, bool releaseOwer = false)
        {
            // Build filter for upsert token for a particular thread
            var builder = Builders<PartitionLease>.Filter;
            var filter = builder.Eq(p => p.processor, lease.processor);
            filter &= builder.Eq(p => p._id, lease._id);

            lease.timeStamp = DateTime.UtcNow;
            lease.owner = !releaseOwer ? _instanceId : "";

            // Upsert the token
            var result = await _leaseCollection.ReplaceOneAsync(filter, lease, new ReplaceOptions() { IsUpsert = true });

            lease.ResetLeaseControl(releaseOwer);
            
            if (_builder._printDebugLogs)
                await Console.Out.WriteLineAsync($"DEBUG: Lease renewed/token updated - {DateTime.UtcNow}");
        }
    }
}