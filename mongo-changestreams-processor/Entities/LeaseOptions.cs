namespace mongo_changestreams_processor.Entities
{
    internal class LeaseOptions
    {
        internal string LeaseDatabaseName { get; set; } = string.Empty;
        internal string LeaseCollectionName { get; set; } = "leases";
        internal int LeaseAcquireInterval { get; set; } = 13000;
        internal int LeaseRenewalInterval { get; set; } = 17000;
        internal int LeaseExpirationInterval { get; set; } = 60000;
        internal bool AllowBalance { get; set; } = true;
    }
}
