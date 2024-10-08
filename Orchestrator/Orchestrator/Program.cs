using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureServices(services =>
    {
        var eventHubPedido = Environment.GetEnvironmentVariable("EventHubPedido");
        var eventHubInventario = Environment.GetEnvironmentVariable("EventHubInventario");
        var eventHubPago = Environment.GetEnvironmentVariable("EventHubPago");

        services.AddSingleton<EventHubProducerClient>(new EventHubProducerClient(eventHubPedido));
        services.AddSingleton<EventHubProducerClient>(new EventHubProducerClient(eventHubInventario));
        services.AddSingleton<EventHubProducerClient>(new EventHubProducerClient(eventHubPago));

        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();
    })
    .Build();

host.Run();
