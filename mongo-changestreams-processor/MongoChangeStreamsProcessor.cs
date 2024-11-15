﻿using Mongo.ChangeStreams.Processor.Entities;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Concurrent;

namespace Mongo.ChangeStreams.Processor
{
    public class MongoChangeStreamsProcessor
    {
        private readonly string _instanceId = Guid.NewGuid().ToString();
        private MongoChangeStreamsProcessorBuilder _builder;

        private MongoClient _mongoClient;
        private IMongoDatabase _database;
        private IMongoCollection<BsonDocument> _collection;
        private IMongoCollection<PartitionLease> _leaseCollection;

        private PartitionLeaseStore _leaseStore;
        private ConcurrentDictionary<string, PartitionLease> _acquiredPartitions = new();
        private ConcurrentDictionary<string, PartitionLease>? _collectionPartitions;

        private string _databaseName;
        private string _collectionName;

        private bool _isRunning = false;
        private ConcurrentDictionary<string, Task> _runningPartitionTasks = new();
        private CancellationTokenSource _cancellation = new();

        internal MongoChangeStreamsProcessor(MongoChangeStreamsProcessorBuilder builder, MongoClient mongoClient, MongoClient leaseClient)
        {
            _builder = builder;
            _mongoClient = mongoClient;

            _databaseName = _builder.databaseName;
            _collectionName = _builder.collectionName;

            // Initialize the processor
            _database = _mongoClient.GetDatabase(_databaseName);
            if (_database == null)
                throw new Exception("Database not found");

            _collection = _database.GetCollection<BsonDocument>(_collectionName);
            if (_collection == null)
                throw new Exception("Collection not found");

            var _leaseDatabase = leaseClient.GetDatabase(_builder.leaseOptions.LeaseDatabaseName);

            if (_leaseDatabase == null)
                throw new Exception("Lease Database not found");

            _leaseCollection = _leaseDatabase.GetCollection<PartitionLease>(_builder.leaseOptions.LeaseCollectionName);

            if (_leaseCollection == null)
                throw new Exception("Lease Collection not found");

            _leaseStore = new(_leaseCollection, _instanceId, _builder.leaseOptions);
        }

        public async Task StartAsync(CancellationToken cancellation)
        {
            if (_isRunning)
                return;

            if (_builder.printDebugLogs)
                await Console.Out.WriteLineAsync($"DEBUG: Instance Id: {_instanceId}");

            // Start the processor
            await AcquireLeaseAsync(cancellation);
            var m = MonitorPartitionsAsync(_cancellation.Token);
            _runningPartitionTasks.TryAdd("monitor", m);

            if (_acquiredPartitions.Count == 0)
                throw new Exception("Could not acquire any partition lease");

            foreach (var partition in _acquiredPartitions)
            {
                // Start the processor for each partition
                var t = ProcessPartitionAsync(partition.Value, _cancellation.Token);
                _runningPartitionTasks.TryAdd(partition.Key, t);
            }

            _isRunning = true;
        }

        public async Task StopAsync()
        {
            if (!_isRunning)
                return;

            // Stop the processor
            _cancellation.Cancel();

            await Task.WhenAll(_runningPartitionTasks.Values);

            _runningPartitionTasks.Clear();
            _isRunning = false;
        }

        internal async Task AcquireLeaseAsync(CancellationToken cancellation)
        {
            do
            {
                _collectionPartitions = new ConcurrentDictionary<string, PartitionLease>(await _leaseStore.GetAllLeasesAsync($"{_builder.processorOptions.ProcessorName}-{_mongoClient.Settings.Server.Host}-{_database.DatabaseNamespace}-{_collectionName}"));

                if (_collectionPartitions.Count > 0)
                {
                    // Use found tokens to resume
                    // Get partitions that have no owners or lease has expired. Filter out partitions that are not owned by this instance.
                    var tempPartitions = _collectionPartitions.Values.Where(x => x.owner == _instanceId || (x.owner == string.Empty && (x.balanceRequest == _instanceId || string.IsNullOrEmpty(x.balanceRequest))) || (x.IsLeaseExpired(_builder.leaseOptions.LeaseExpirationInterval))).ToList();

                    if (tempPartitions.Count() == 0)
                    {
                        if (_builder.processorOptions.AllowBalance && _collectionPartitions.Count > 1)
                        {
                            var balanceIntent = _collectionPartitions.Values.GroupBy(x => x.owner).Where(x => x.Count() > 1).ToList();

                            if (balanceIntent.Count > 0)
                            {
                                var number = Math.Floor(_collectionPartitions.Count / (balanceIntent.Count + 1) * 1F);

                                foreach (var partition in _collectionPartitions.TakeLast((int)number))
                                {
                                    if (_builder.printDebugLogs)
                                        await Console.Out.WriteLineAsync($"DEBUG: Requesting balance intent on partition {partition.Key}");
                                    await _leaseStore.RequestBalanceIntentAsync(partition.Value);
                                }
                            }
                        }

                        if (_builder.printDebugLogs)
                            await Console.Out.WriteLineAsync("DEBUG: Lease not acquired. Sleeping...");

                        _collectionPartitions.Clear();

                        await Task.Delay(_builder.leaseOptions.LeaseAcquireInterval, cancellation);
                        continue;
                    }

                    foreach (var partition in tempPartitions)
                    {
                        var leasedPartition = await _leaseStore.AcquireLeaseAsync(partition, _builder.leaseOptions.LeaseExpirationInterval);
                        if (leasedPartition != null)
                            _acquiredPartitions.TryAdd(leasedPartition._id, leasedPartition);

                        if (_builder.printDebugLogs)
                            await Console.Out.WriteLineAsync($"DEBUG: Acquired lease on partition {partition._id}");
                    }
                }
                else
                {
                    // If no leases found, get tokens from database
                    _collectionPartitions = new ConcurrentDictionary<string, PartitionLease>(await GetPartitionTokensAsync(cancellation));

                    foreach (var partition in _collectionPartitions)
                    {
                        if (await _leaseStore.CreateNewLeaseAsync(partition.Value))
                            _acquiredPartitions.TryAdd(partition.Key, partition.Value);
                    }
                }

            } while (_acquiredPartitions.Count == 0 && !cancellation.IsCancellationRequested);
        }

        internal async Task<Dictionary<string, PartitionLease>> GetPartitionTokensAsync(CancellationToken cancellation)
        {
            Dictionary<string, PartitionLease> partitions = new();

            if (_builder.processorOptions.IsCosmosRU)
            {
                var initialTimestamp = _builder.processorOptions.StartFromBeginning ? DateTime.MinValue : _builder.processorOptions.StartTime;

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
                        _id = $"{_builder.processorOptions.ProcessorName}-{counter}-{_mongoClient.Settings.Server.Host}-{_database.DatabaseNamespace}-{_collectionName}",
                        processor = $"{_builder.processorOptions.ProcessorName}-{_mongoClient.Settings.Server.Host}-{_database.DatabaseNamespace}-{_collectionName}",
                        owner = _instanceId,
                        partitionNumber = counter,
                        token = p.AsBsonDocument
                    };
                    partitions.Add(partition._id, partition);
                    counter++;
                }
            }
            else
            {
                var partition = new PartitionLease()
                {
                    _id = $"{_builder.processorOptions.ProcessorName}-1-{_mongoClient.Settings.Server.Host}-{_database.DatabaseNamespace}-{_collectionName}",
                    processor = $"{_builder.processorOptions.ProcessorName}-{_mongoClient.Settings.Server.Host}-{_database.DatabaseNamespace}-{_collectionName}",
                    owner = _instanceId,
                    partitionNumber = 1
                };
                partitions.Add(partition._id, partition);
            }

            return partitions;
        }

        internal async Task ProcessPartitionAsync(PartitionLease lease, CancellationToken cancellation)
        {
            bool leaseRenewalRequired = false;

            var projection = Builders<ChangeStreamDocument<BsonDocument>>.Projection
                .Include("_id")
                .Include("fullDocument")
                .Include("ns")
                .Include("documentKey");

            if(!_builder.processorOptions.IsCosmosRU)
                projection.Include("operationType")
                    .Include("updateDescription");

            // Create a pipeline
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
                .Match(change => change.OperationType == ChangeStreamOperationType.Insert || change.OperationType == ChangeStreamOperationType.Update || change.OperationType == ChangeStreamOperationType.Replace)
                .Project(projection);

            // Create options
            var options = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                BatchSize = _builder.processorOptions.BatchSize, //Define the batch size
            };

            if(lease.token != null)
                options.ResumeAfter = lease.token; // Provide the resume tokens to watcher

            if (!_builder.processorOptions.IsCosmosRU && lease.token == null)
            {
                var startDate = (DateTimeOffset)(_builder.processorOptions.StartFromBeginning ? DateTime.MinValue : _builder.processorOptions.StartTime);
                //Not Yet Supported on vCore: options.StartAtOperationTime = BsonTimestamp.Create(startDate.ToUnixTimeSeconds());
            }

            try
            {
                // Watch the collection
                using (var cursor = await _collection.WatchAsync(pipeline, options, cancellation))
                {
                    var retryAttempts = 0;
                    // Loop through the changes
                    while (!cancellation.IsCancellationRequested && !lease.IsReleaseLeaseRequested)
                    {
                        if (retryAttempts == 0)
                            await cursor.MoveNextAsync();

                        leaseRenewalRequired = false;
                        try
                        {
                            if (cursor.Current != null && cursor.Current.Count() > 0)
                            {
                                try
                                {
                                    // Handle received list of documents
                                    await _builder.processorOptions.OnChangesHandler(cursor.Current, cancellation);

                                    // Get resume token for this batch
                                    lease.token = cursor.GetResumeToken();

                                    leaseRenewalRequired = true;
                                    retryAttempts = 0;
                                }
                                catch(Exception ex)
                                {
                                    if(retryAttempts < _builder.processorOptions.MaxBatchRetryAttempts || _builder.processorOptions.MaxBatchRetryAttempts == -1)
                                    {
                                        retryAttempts++;
                                        leaseRenewalRequired = false;

                                        if (_builder.printDebugLogs)
                                            await Console.Out.WriteLineAsync($"DEBUG: Exception thrown by delegate! Retry: {retryAttempts}{Environment.NewLine}Message: {ex.Message}{Environment.NewLine}Stack Trace: {ex.StackTrace}");

                                        await Task.Delay(_builder.processorOptions.RetryAttemptInterval, cancellation);
                                    }
                                    else
                                    {
                                        retryAttempts = 0;
                                        leaseRenewalRequired = true;
                                        // Get resume token for this batch
                                        lease.token = cursor.GetResumeToken();

                                        if (_builder.printDebugLogs)
                                            await Console.Out.WriteLineAsync($"DEBUG: Max retry attempts reached!{Environment.NewLine}Message: {ex.Message}{Environment.NewLine}Stack Trace: {ex.StackTrace}");
                                    }                                    
                                }                                
                            }
                            else
                            {
                                leaseRenewalRequired = lease.IsLeaseRenewalRequired(_builder.leaseOptions.LeaseRenewalInterval);
                            }

                            if (leaseRenewalRequired)
                            {
                                await _leaseStore.StoreResumeToken(lease);
                                if (_builder.printDebugLogs)
                                    await Console.Out.WriteLineAsync($"DEBUG: Token/lease refreshed on partition {lease._id}");
                            }
                        }
                        catch (Exception ex)
                        {
                            await Console.Out.WriteLineAsync(ex.Message);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                await Console.Out.WriteLineAsync(ex.Message);
            }
            finally
            {
                await _leaseStore.ReleaseLeaseAsync(lease);
                _acquiredPartitions.Remove(lease._id, out var p);
                _runningPartitionTasks.Remove(lease._id, out var task);
                if (_builder.printDebugLogs)
                    await Console.Out.WriteLineAsync($"DEBUG: Released partition {lease._id}, AcquiredPartitionsCount: {_acquiredPartitions.Count}, RunningTasks: {_runningPartitionTasks.Count}");
            }
        }

        internal async Task MonitorPartitionsAsync(CancellationToken cancellation)
        {
            while (!cancellation.IsCancellationRequested)
            {
                if (_collectionPartitions != null && _collectionPartitions.Count != _acquiredPartitions.Count)
                {
                    foreach (var key in _collectionPartitions.Keys.Except(_acquiredPartitions.Keys))
                    {
                        var leasedPartition = await _leaseStore.AcquireLeaseAsync(_collectionPartitions[key], _builder.leaseOptions.LeaseExpirationInterval);
                        if (leasedPartition != null)
                        {
                            var t = ProcessPartitionAsync(leasedPartition, _cancellation.Token);
                            _runningPartitionTasks.TryAdd(key, t);

                            _acquiredPartitions.TryAdd(leasedPartition._id, leasedPartition);
                        }

                        if (_builder.printDebugLogs)
                            await Console.Out.WriteLineAsync($"DEBUG: Partition Monitor {key}, Status: {(leasedPartition == null ? "Skipped" : "Acquired")}, AcquiredPartitionsCount: {_acquiredPartitions.Count}, RunningTasks: {_runningPartitionTasks.Count}");
                    }
                }

                try
                {
                    await Task.Delay(_builder.leaseOptions.LeaseAcquireInterval, cancellation);
                }
                catch (TaskCanceledException)
                {
                    // Do nothing
                }
            }

            _runningPartitionTasks.Remove("monitor", out var task);
            if (_builder.printDebugLogs)
                await Console.Out.WriteLineAsync($"DEBUG: Partition Monitor stopped! RunningTasks: {_runningPartitionTasks.Count}");
        }
    }
}