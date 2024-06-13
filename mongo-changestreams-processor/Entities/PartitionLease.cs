using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Diagnostics;

namespace mongo_changestreams_processor.Entities
{
    internal class PartitionLease
    {
        [BsonId]
        [BsonRepresentation(BsonType.String)]
        public string _id { get; set; }
        public string processor { get; set; }
        public string owner { get; set; }
        public int partitionNumber { get; set; }
        public BsonDocument token { get; set; }
        public DateTime timeStamp { get; set; }
        public string balanceControl { get; set; } = "None";

        internal Stopwatch leaseControl = new();

        internal bool IsLeaseExpired(int leaseExpirationInterval)
        {
            return timeStamp.AddMilliseconds(leaseExpirationInterval) < DateTime.UtcNow;
        }

        internal void ResetLeaseControl(bool stop)
        {
            if (stop)
            {
                leaseControl.Stop();
            }
            else
            {
                leaseControl.Restart();
            }
        }

        internal bool IsLeaseRenewalRequired(int leaseRenewalInterval)
        {
            return leaseControl.ElapsedMilliseconds > leaseRenewalInterval;
        }
    }
}