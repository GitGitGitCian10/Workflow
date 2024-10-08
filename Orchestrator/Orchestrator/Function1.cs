using System;
using System.Collections.Concurrent;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Orchestrator
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        private readonly EventHubProducerClient _eventHubPedido;
        private readonly EventHubProducerClient _eventHubInventario;
        private readonly EventHubProducerClient _eventHubPago;

        public Function1(ILogger<Function1> logger, EventHubProducerClient eventHubPedido, EventHubProducerClient eventHubInventario, EventHubProducerClient eventHubPago)
        {
            _logger = logger;
            _eventHubPedido = eventHubPedido;
            _eventHubInventario = eventHubInventario;
            _eventHubPago = eventHubPago;
        }

        [Function(nameof(Function1))]
        public void Run([EventHubTrigger("evh-orch", Connection = "EventHubSrc")] EventData[] events)
        {
            foreach (EventData @event in events)
            {
                string jsonString = System.Text.Encoding.UTF8.GetString(@event.EventBody.ToArray());
                _logger.LogInformation($"Received message: {jsonString}");

                dynamic jsonObject = JsonConvert.DeserializeObject(jsonString);

                _logger.LogInformation($"Evento tipo: {jsonObject.EventType}");
            }
        }
    }
}
