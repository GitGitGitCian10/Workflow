using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace FunctionApp1
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        private readonly EventHubProducerClient _producerClient;

        public Function1(ILogger<Function1> logger, EventHubProducerClient producerClient)
        {
            _logger = logger;
            _producerClient = producerClient;
        }

        [Function(nameof(Function1))]
        public async Task Run([EventHubTrigger("evh-pedido", Connection = "EventHubSrc")] EventData[] events)
        {
            foreach (EventData @event in events)
            {
                string jsonString = System.Text.Encoding.UTF8.GetString(@event.EventBody.ToArray());
                _logger.LogInformation($"Received message: {jsonString}");

                dynamic jsonObject = JsonConvert.DeserializeObject(jsonString);
                jsonObject.EventType = "SuccessPedido";

                string modifiedJsonString = JsonConvert.SerializeObject(jsonObject);
                _logger.LogInformation($"{modifiedJsonString}");

                using var batch = await _producerClient.CreateBatchAsync();
                batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(modifiedJsonString)));

                await _producerClient.SendAsync(batch);
                _logger.LogInformation("Sent modified message to evh-orch");
            }
        }
    }
}