using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureServices(services =>
    {

        services.AddSingleton(new CosmosClient(Environment.GetEnvironmentVariable("CosmosDBConnectionString")));
        services.AddSingleton(new EventHubProducerClient(Environment.GetEnvironmentVariable("EventHubDest"), "evh-orch"));
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();
    })
    .Build();

host.Run();
