using MongoDB.Bson;

namespace Mongo.ChangeStreams.Processor.Entities
{
    internal class ProcessorOptions
    {
        internal bool IsCosmosRU { get; set; } = false;
        internal bool AllowBalance { get; set; } = true;
        internal string ProcessorName { get; set; } = "processor";
        internal bool StartFromBeginning { get; set; } = false;
        internal DateTime StartTime { get; set; } = DateTime.MinValue;
        internal int BatchSize { get; set; } = 100;
        internal int MaxBatchRetryAttempts { get; set; } = -1;
        internal int RetryAttemptInterval { get; set; } = 1000;
        internal Func<IEnumerable<BsonDocument>, CancellationToken, Task> OnChangesHandler { get; set; } = async (changes, token) =>
        {
            await Task.Yield();
            return;
        };
    }
}
