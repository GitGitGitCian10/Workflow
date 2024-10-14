using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Azure.Cosmos;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureServices(services =>
    {
        var eventHubPedido = Environment.GetEnvironmentVariable("EventHubPedido");
        var eventHubInventario = Environment.GetEnvironmentVariable("EventHubInventario");
        var eventHubPago = Environment.GetEnvironmentVariable("EventHubPago");

        services.AddSingleton(new CosmosClient(Environment.GetEnvironmentVariable("CosmosDBConnectionString")));

        services.AddSingleton<IDictionary<string, EventHubProducerClient>>(sp => new Dictionary<string, EventHubProducerClient>
        {
            { "Pedido", new EventHubProducerClient(eventHubPedido) },
            { "Inventario", new EventHubProducerClient(eventHubInventario) },
            { "Pago", new EventHubProducerClient(eventHubPago) }
        });

        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();
    })
    .Build();

host.Run();