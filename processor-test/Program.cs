﻿using Microsoft.Extensions.Configuration;
using mongo_changestreams_processor;
using MongoDB.Bson;
using MongoDB.Driver;

public partial class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("Starting processor...");
        Console.CancelKeyPress += Console_CancelKeyPress;

        //read appsettings.json
        var config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();
        var mongoConnectionString = config["MongoConnection"];
        var databaseName = config["databaseName"];
        var collectionName = config["collectionName"];

        if (string.IsNullOrEmpty(mongoConnectionString) || string.IsNullOrEmpty(databaseName) || string.IsNullOrEmpty(collectionName))
        {
            Console.WriteLine("Invalid configuration");
            return;
        }

        MongoClient mongoClient = new(mongoConnectionString);

        var processor = MongoChangeStreamsProcessorBuilder.Create()
            .WithMongoClient(mongoClient)
            .WithDatabase(databaseName)
            .WithCollection(collectionName)
            .WithProcessorName("consumer1")
            //.WithLeaseExpirationInterval(60000)
            //.WithLeaseRenewalInterval(30000)
            .Build();

        try
        {
            await processor.StartAsync(ProcessAsync);
            await Task.Delay(-1, cts.Token);
        }
        catch (TaskCanceledException)
        {
            await processor.StopAsync();
            Console.WriteLine("Stopped!");
        }
    }

    static CancellationTokenSource cts = new();
    

    static void Console_CancelKeyPress(object? sender, ConsoleCancelEventArgs e)
    {
        Console.WriteLine("Stopping...");
        e.Cancel = true;
        cts.Cancel();
    }

    static async Task ProcessAsync(IEnumerable<BsonDocument> changes, CancellationToken cancellationToken)
    {
        foreach (var change in changes)
        {
            await Console.Out.WriteLineAsync(change.ToJson());
        }
    }
}