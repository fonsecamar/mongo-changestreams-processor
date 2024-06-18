using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Diagnostics;

namespace Mongo.ChangeStreams.Processor.Entities
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
        public string balanceRequest { get; set; } = "";

        internal Stopwatch leaseControl = new();

        internal bool IsLeaseExpired(int leaseExpirationInterval)
        {
            return timeStamp.AddMilliseconds(leaseExpirationInterval) < DateTime.UtcNow;
        }

        internal void ResetLeaseControl(bool stop = false)
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

        internal bool IsReleaseLeaseRequested
        {
            get
            {
                return !string.IsNullOrEmpty(balanceRequest) && balanceRequest != owner;
            }
        }
    }
}