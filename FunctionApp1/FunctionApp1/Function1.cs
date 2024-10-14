using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos;

namespace Pedido
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        private readonly CosmosClient _cosmosClient;

        private readonly EventHubProducerClient _producerClient;

        public Function1(ILogger<Function1> logger, CosmosClient cosmosClient, EventHubProducerClient producerClient)
        {
            _logger = logger;
            _cosmosClient = cosmosClient;
            _producerClient = producerClient;
        }

        [Function(nameof(Function1))]
        public async Task Run([EventHubTrigger("evh-pedido", Connection = "EventHubSrc")] EventData[] events)
        {
            foreach (EventData @event in events)
            {
                string jsonString = System.Text.Encoding.UTF8.GetString(@event.EventBody.ToArray());
                _logger.LogInformation($"Received message: {jsonString}");

                using var batch = await _producerClient.CreateBatchAsync();
                dynamic jsonObject = JsonConvert.DeserializeObject(jsonString);

                var container = _cosmosClient.GetContainer("cdb-pedido", "Container1");

                if(jsonObject.EventType == "CrearPedido")
                {
                    var queryString = "SELECT * FROM c WHERE c.OrderId = '" + jsonObject.OrderId + "'";
                    bool cloned = false;
                    using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                    {
                        while (iterator.HasMoreResults)
                        {
                            FeedResponse<dynamic>  response = await iterator.ReadNextAsync();
                            if (response.Count != 0) cloned = true;
                        }
                    }
                    if ((Convert.ToInt32(jsonObject.Quantity) > 0) && !cloned)
                    {
                        var myObj = new { id = jsonObject.id, OrderId = jsonObject.OrderId, ProductId = jsonObject.ProductId, Quantity = jsonObject.Quantity, Status = "Pending" };

                        await container.CreateItemAsync(myObj);
                            
                        jsonObject.EventType = "SuccessCrearPedido";
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }
                    else
                    {
                        jsonObject.EventType = "ErrorCrearPedido";
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }                 
                }
                else if(jsonObject.EventType == "CancelarPedido")
                {
                    var queryString = "SELECT * FROM c WHERE c.OrderId = '" + jsonObject.OrderId + "'";
                    bool exists = false;
                    using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                    {
                        while (iterator.HasMoreResults)
                        {
                            FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                            if (response.Count > 0) exists = true;
                        }
                    }
                    if(exists)
                    {
                        queryString = "SELECT * FROM c WHERE c.OrderId = '" + jsonObject.OrderId + "'";
                        dynamic result = 0;
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        result.Status = "Cancelled";
                        await container.ReplaceItemAsync(result, result.id.ToString());

                        jsonObject.EventType = "SuccessCancelarPedido";
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }
                    else
                    {
                        jsonObject.EventType = "ErrorCancelarPedido";
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }   
                }
                else if (jsonObject.EventType == "CompletarPedido")
                {
                    var queryString = "SELECT * FROM c WHERE c.OrderId = '" + jsonObject.OrderId + "'";
                    bool exists = false;
                    using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                    {
                        while (iterator.HasMoreResults)
                        {
                            FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                            if (response.Count > 0) exists = true;
                        }
                    }
                    if (exists)
                    {
                        queryString = "SELECT * FROM c WHERE c.OrderId = '" + jsonObject.OrderId + "'";
                        dynamic result = 0;
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        result.Status = "Completed";
                        await container.ReplaceItemAsync(result, result.id.ToString());

                        jsonObject.EventType = "SuccessCompletarPedido";
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }
                    else
                    {
                        jsonObject.EventType = "ErrorCompletarPedido";
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }
                }

                await _producerClient.SendAsync(batch);
            }
        }
    }
}