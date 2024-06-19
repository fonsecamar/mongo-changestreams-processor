# Azure Cosmos DB for Mongo - Change Streams Processor

## Introduction

This library helps consuming change streams data from Azure Cosmos DB for Mongo automatically scalling based on the number of Cosmos DB underlying physical partitions, keeping track of continuation tokens per partition and enabling balancing consumer across multiple instances up to the number of physical partitions.

## Usage

```csharp
CancellationTokenSource cts = new();

var processor = MongoChangeStreamsProcessorBuilder.Create()
    .WithMongoClient(mongoClient)
    .WithDatabase(databaseName)
    .WithCollection(collectionName)
    .WithProcessorName("consumerName", ProcessAsync)
    .Build();

try
{
    await processor.StartAsync(cts.Token);
}
catch (Exception)
{
    await processor.StopAsync();
}

async Task ProcessAsync(IEnumerable<BsonDocument> changes, CancellationToken cancellationToken)
{
    foreach (var change in changes)
    {
        await Console.Out.WriteLineAsync(change.ToJson());
    }
}
```

## Configuration Options

| Builder Method | Parameter Type | Description | Required |
| --- | --- | --- | --- |
| WithMongoClient | MongoClient | Mongo Client instance to monitored account | yes |
| WithDatabase | string | Database name hosting monitored collection | yes |
| WithCollection | string | Monitored collection name | yes |
| WithProcessorName | (string, Func<IEnumerable<BsonDocument>, CancellationToken, Task>) | Processor name and delegate invoked on changes | yes |
| WithLeaseClient | MongoClient | Mongo Client instance to leases account (Default: monitored account client) | no |
| WithLeaseDatabase | string | Lease database name (Default: monitored database) | no |
| WithLeaseCollection | string | Leases collection. Must exists and should be either unsharded or sharded by /processor attribute (Default: "leases") | no |
| WithDisableBalance | none | Disables balance across multiple processor instances with same processor name | no |
| WithStartFromBeginning | none | Starts change streams from the beginning | no |
| WithStartTime | DateTime | Starts change streams from a specific date time (not compatible with StartFromBeginning) | no |
| WithBatchSize | int | Defines maximum batch size returned by IEnumerable<BsonDocument> (Default: 100) | no |
| WithMaxBatchRetryAttempts | int | Defines maximum retry attempts in case of client failure to handle changes (Default: -1 - infinite) | no |
| WithRetryAttemptInterval | int | Defines interval in milliseconds between retry attemps to handle changes (Default: 1000) | no |
| WithLeaseAcquireInterval | int | Defines interval in milliseconds between attemps to acquire lease (Default: 13000) | no |
| WithLeaseRenewalInterval | int | Defines interval in milliseconds to force lease renewal if no changes happened (Default: 17000) | no |
| WithLeaseExpirationInterval | int | Defines interval in milliseconds to consider a lease expired (Default: 60000) | no |
| WithDebugLogs | none | Print logs to console (noisy) | no |

<br/>

## How to Contribute

If you find any errors or have suggestions for changes, please be part of this project!

1. Create your branch: `git checkout -b my-new-feature`
2. Add your changes: `git add .`
3. Commit your changes: `git commit -m '<message>'`
4. Push your branch to Github: `git push origin my-new-feature`
5. Create a new Pull Request 😄