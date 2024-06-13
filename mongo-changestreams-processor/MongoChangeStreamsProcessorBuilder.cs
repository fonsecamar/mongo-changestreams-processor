using MongoDB.Bson;
using MongoDB.Driver;

namespace mongo_changestreams_processor
{
    public class MongoChangeStreamsProcessorBuilder
    {
        internal MongoChangeStreamsProcessorBuilder()
        {
        }

        private MongoClient? _mongoClient = null;
        internal MongoClient? _leaseClient = null;

        internal string _databaseName = string.Empty;
        internal string _collectionName = string.Empty;

        internal string? _leaseDatabaseName;
        internal string? _leaseCollectionName;

        internal string _processorName = string.Empty;

        internal bool _startFromBeginning;
        internal DateTime _startTime = DateTime.MinValue;

        internal int _batchSize = 100;

        internal int _leaseRenewalInterval = 17000;
        internal int _leaseExpirationInterval = 60000;

        internal bool _allowBalance = true;
        internal bool _printDebugLogs = false;

        private static readonly MongoChangeStreamsProcessorBuilder _builder = new MongoChangeStreamsProcessorBuilder();

        public static MongoChangeStreamsProcessorBuilder Create()
        {
            return new();
        }

        public MongoChangeStreamsProcessorBuilder WithMongoClient(MongoClient mongoClient)
        {
            _mongoClient = mongoClient;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithDatabase(string databaseName)
        {
            _databaseName = databaseName;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithCollection(string collectionName)
        {
            _collectionName = collectionName;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseClient(MongoClient leaseClient)
        {
            _leaseClient = leaseClient;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseDatabase(string leaseDatabaseName)
        {
            _leaseDatabaseName = leaseDatabaseName;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseCollection(string leaseCollectionName)
        {
            _leaseCollectionName = leaseCollectionName;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithDisableBalance()
        {
            _allowBalance = false;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithProcessorName(string processorName)
        {
            _processorName = processorName;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithStartFromBeginning()
        {
            _startFromBeginning = true;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithStartTime(DateTime startTime)
        {
            _startTime = startTime;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithBatchSize(int batchSize)
        {
            _batchSize = batchSize;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseRenewalInterval(int leaseRenewalInterval)
        {
            _leaseRenewalInterval = leaseRenewalInterval;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithLeaseExpirationInterval(int leaseExpirationInterval)
        {
            _leaseExpirationInterval = leaseExpirationInterval;
            return this;
        }

        public MongoChangeStreamsProcessorBuilder WithDebugLogs()
        {
            _printDebugLogs = true;
            return this;
        }

        public MongoChangeStreamsProcessor Build()
        {
            if (_mongoClient == null)
            {
                throw new InvalidOperationException("MongoClient must be set before building.");
            }

            return new MongoChangeStreamsProcessor(this, _mongoClient);
        }
    }
}