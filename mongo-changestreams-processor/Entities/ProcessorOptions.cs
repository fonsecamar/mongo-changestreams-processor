using MongoDB.Bson;

namespace mongo_changestreams_processor.Entities
{
    internal class ProcessorOptions
    {
        internal bool IsCosmosRU { get; set; } = false;
        internal bool AllowBalance { get; set; } = true;
        internal string ProcessorName { get; set; } = "processor";
        internal bool StartFromBeginning { get; set; } = false;
        internal DateTime StartTime { get; set; } = DateTime.MinValue;
        internal int BatchSize { get; set; } = 100;
        internal Func<IEnumerable<BsonDocument>, CancellationToken, Task> OnChangesHandler { get; set; } = async (changes, token) =>
        {
            await Task.Yield();
            return;
        };
    }
}
