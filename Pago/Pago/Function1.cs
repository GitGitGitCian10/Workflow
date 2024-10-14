using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos;

namespace FunctionApp1
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        private readonly CosmosClient _cosmosClient;

        private readonly EventHubProducerClient _producerClient;

        public Function1(ILogger<Function1> logger, CosmosClient cosmoClient, EventHubProducerClient producerClient)
        {
            _logger = logger;
            _cosmosClient = cosmoClient;
            _producerClient = producerClient;
        }

        [Function(nameof(Function1))]
        public async Task Run([EventHubTrigger("evh-pago", Connection = "EventHubSrc")] EventData[] events)
        {
            foreach (EventData @event in events)
            {
                string jsonString = System.Text.Encoding.UTF8.GetString(@event.EventBody.ToArray());
                _logger.LogInformation($"Received message: {jsonString}");

                using var batch = await _producerClient.CreateBatchAsync();
                dynamic jsonObject = JsonConvert.DeserializeObject(jsonString);

                var container = _cosmosClient.GetContainer("cdb-pago", "Container1");

                if (jsonObject.EventType == "RealizarPago")
                {
                    var queryString = "SELECT * FROM c WHERE c.AccountId = '" + jsonObject.AccountId + "'";
                    bool exists = false;
                    using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                    {
                        while (iterator.HasMoreResults)
                        {
                            FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                            if (response.Count != 0) exists = true;
                        }
                    }
                    if (exists)
                    {
                        queryString = "SELECT * FROM c WHERE c.AccountId = '" + jsonObject.AccountId + "'";
                        dynamic result = 0;
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        if(result.Balance - Convert.ToInt32(jsonObject.Price) >= 0)
                        {
                            result.Balance -= Convert.ToInt32(jsonObject.Price);
                            await container.ReplaceItemAsync(result, result.id.ToString());

                            jsonObject.EventType = "SuccessRealizarPago";
                            string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                            batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                        }
                        else
                        {
                            jsonObject.EventType = "ErrorRealizarPago";
                            string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                            batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                        }
                    }
                    else
                    {
                        jsonObject.EventType = "ErrorRealizarPago";
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }
                }
                else if (jsonObject.EventType == "DeshacerPago")
                {
                    var queryString = "SELECT * FROM c WHERE c.AccountId = '" + jsonObject.AccountId + "'";
                    bool exists = false;
                    using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                    {
                        while (iterator.HasMoreResults)
                        {
                            FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                            if (response.Count != 0) exists = true;
                        }
                    }
                    if (exists)
                    {
                        queryString = "SELECT * FROM c WHERE c.AccountId = '" + jsonObject.AccountId + "'";
                        dynamic result = 0;
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        result.Balance += Convert.ToInt32(jsonObject.Price);
                        await container.ReplaceItemAsync(result, result.id.ToString());

                        jsonObject.EventType = "SuccessDeshacerPago";
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }
                    else
                    {
                        jsonObject.EventType = "ErrorDeshacerPago";
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }
                }
                
                await _producerClient.SendAsync(batch);
            }
        }
    }
}