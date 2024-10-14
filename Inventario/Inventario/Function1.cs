using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace FunctionApp1
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        private readonly EventHubProducerClient _producerClient;

        private readonly CosmosClient _cosmosClient;

        public Function1(ILogger<Function1> logger, CosmosClient cosmosClient, EventHubProducerClient producerClient)
        {
            _logger = logger;
            _cosmosClient = cosmosClient;
            _producerClient = producerClient;
        }

        [Function(nameof(Function1))]
        public async Task Run([EventHubTrigger("evh-inventario", Connection = "EventHubSrc")] EventData[] events)
        {
            foreach (EventData @event in events)
            {
                string jsonString = System.Text.Encoding.UTF8.GetString(@event.EventBody.ToArray());

                using var batch = await _producerClient.CreateBatchAsync();
                dynamic jsonObject = JsonConvert.DeserializeObject(jsonString);

                var container = _cosmosClient.GetContainer("cdb-inventario", "Container1");

                if (jsonObject.EventType == "ReservaStock")
                {
                    var queryString = "SELECT VALUE c.Quantity FROM c WHERE c.ProductId = '" + jsonObject.ProductId + "'";

                    int quantity = 0;

                    using (var queryResultSetIterator = container.GetItemQueryIterator<int>(queryString))
                    {
                        if (queryResultSetIterator.HasMoreResults)
                        {
                            var response = await queryResultSetIterator.ReadNextAsync();

                            quantity = response.Resource[0];
                        }
                    }
                    if(quantity - Convert.ToInt32(jsonObject["Quantity"]) >= 0)
                    {
                        queryString = "SELECT * FROM c WHERE c.ProductId = '" + jsonObject.ProductId + "'";
                        dynamic result = 0;
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        result.Quantity -= Convert.ToInt32(jsonObject["Quantity"]);
                        await container.ReplaceItemAsync(result, result.id.ToString(), new PartitionKey(result.ProductId.ToString()));

                        jsonObject.EventType = "SuccessReservaStock";
                        jsonObject.Price = result.Price * Convert.ToInt32(jsonObject["Quantity"]);
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }
                    else
                    {
                        jsonObject.EventType = "ErrorReservaStock";
                        string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                        batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                    }    
                }
                else if(jsonObject.EventType == "UndoStock")
                {
                    var queryString = "SELECT * FROM c WHERE c.ProductId = '" + jsonObject.ProductId + "'";
                    dynamic result = 0;
                    using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                    {
                        while (iterator.HasMoreResults)
                        {
                            FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                            result = response.Resource.First();
                        }
                    }
                    result.Quantity += Convert.ToInt32(jsonObject["Quantity"]);
                    await container.ReplaceItemAsync(result, result.id.ToString(), new PartitionKey(result.ProductId.ToString()));

                    jsonObject.EventType = "SuccessUndoStock";
                    string jsonToSend = JsonConvert.SerializeObject(jsonObject);

                    batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));
                }

                await _producerClient.SendAsync(batch);
            }
        }
    }
}
