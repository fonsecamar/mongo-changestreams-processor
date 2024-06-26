﻿using Mongo.ChangeStreams.Processor.Entities;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Mongo.ChangeStreams.Processor
{
    public class MongoChangeStreamsProcessorBuilder
    {
        private MongoChangeStreamsProcessorBuilder()
        {
        }

        private MongoClient? mongoClient = null;
        private MongoClient? leaseClient = null;

        internal string databaseName = string.Empty;
        internal string collectionName = string.Empty;

        internal bool printDebugLogs = false;

        internal readonly LeaseOptions leaseOptions = new();
        internal readonly ProcessorOptions processorOptions = new();

        public static MongoChangeStreamsProcessorBuilder Create()
        {
            return new();
        }

        public MongoChangeStreamsProcessorBuilder WithMongoClient(MongoClient mongoClient)
        {
            this.mongoClient = mongoClient;
            this.processorOptions.IsCosmosRU = mongoClient.Settings.Server.Host.ToLowerInvariant().Contains(".mongo.cosmos.azure.com");
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithDatabase(string databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithCollection(string collectionName)
        {
            this.collectionName = collectionName;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithProcessorName(string processorName, Func<IEnumerable<BsonDocument>, CancellationToken, Task> onChangesHandler)
        {
            if (string.IsNullOrEmpty(processorName))
            {
                throw new System.ArgumentException("Processor name cannot be null or empty", nameof(processorName));
            }

            if (onChangesHandler == null)
            {
                throw new System.ArgumentException("OnChangesHandler cannot be null", nameof(onChangesHandler));
            }

            this.processorOptions.ProcessorName = processorName;
            this.processorOptions.OnChangesHandler = onChangesHandler;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseClient(MongoClient leaseClient)
        {
            this.leaseClient = leaseClient;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseDatabase(string leaseDatabaseName)
        {
            this.leaseOptions.LeaseDatabaseName = leaseDatabaseName;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseCollection(string leaseCollectionName)
        {
            if (string.IsNullOrEmpty(leaseCollectionName))
            {
                throw new System.ArgumentException("Lease collection name cannot be null or empty", nameof(leaseCollectionName));
            }

            this.leaseOptions.LeaseCollectionName = leaseCollectionName;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithDisableBalance()
        {
            this.processorOptions.AllowBalance = this.leaseOptions.AllowBalance = false;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithStartFromBeginning()
        {
            this.processorOptions.StartFromBeginning = true;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithStartTime(DateTime startTime)
        {
            if(this.processorOptions.StartFromBeginning)
            {
                throw new System.ArgumentException("Start time cannot be used with StartFromBeginning.", nameof(startTime));
            }

            this.processorOptions.StartTime = startTime;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithBatchSize(int batchSize)
        {
            if (batchSize <= 0)
            {
                throw new System.ArgumentException("Batch size must be greater than 0", nameof(batchSize));
            }

            this.processorOptions.BatchSize = batchSize;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithMaxBatchRetryAttempts(int maxBatchRetryAttempts)
        {
            if (maxBatchRetryAttempts < -1)
            {
                throw new System.ArgumentException("Max batch retry attempts must be greater than -1", nameof(maxBatchRetryAttempts));
            }

            this.processorOptions.MaxBatchRetryAttempts = maxBatchRetryAttempts;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithRetryAttemptInterval(int retryAttemptInterval)
        {
            if (retryAttemptInterval < 0 || retryAttemptInterval > 1000)
            {
                throw new System.ArgumentException("Retry attempt interval must be between 0 and 1000", nameof(retryAttemptInterval));
            }

            this.processorOptions.RetryAttemptInterval = retryAttemptInterval;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseAcquireInterval(int leaseAcquireInterval)
        {
            this.leaseOptions.LeaseAcquireInterval = leaseAcquireInterval;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseRenewalInterval(int leaseRenewalInterval)
        {
            this.leaseOptions.LeaseRenewalInterval = leaseRenewalInterval;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseExpirationInterval(int leaseExpirationInterval)
        {
            this.leaseOptions.LeaseExpirationInterval = leaseExpirationInterval;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithDebugLogs()
        {
            this.printDebugLogs = true;
            return this;
        }

        public MongoChangeStreamsProcessor Build()
        {
            if (this.mongoClient == null)
            {
                throw new InvalidOperationException("MongoClient must be set before building.");
            }

            if(this.leaseClient == null)
            {
                this.leaseClient = this.mongoClient;
            }

            if (string.IsNullOrEmpty(this.databaseName) || string.IsNullOrEmpty(this.collectionName))
            {
                throw new InvalidOperationException("Database and Collection must be set before building.");
            }

            if(string.IsNullOrEmpty(this.leaseOptions.LeaseDatabaseName))
            {
                this.leaseOptions.LeaseDatabaseName = this.databaseName;
            }

            return new MongoChangeStreamsProcessor(this, mongoClient, leaseClient);
        }
    }
}