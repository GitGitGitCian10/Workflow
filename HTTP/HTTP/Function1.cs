using Azure.Messaging.EventHubs.Producer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text.Json.Nodes;
using System.Text;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Cosmos.Serialization.HybridRow;
using System.Runtime.InteropServices.JavaScript;

namespace HTTP
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        private readonly CosmosClient _cosmoClient;

        private readonly EventHubProducerClient _eventHubOrch;

        public Function1(ILogger<Function1> logger, CosmosClient cosmoClient, EventHubProducerClient eventHubProducerClient)
        {
            _logger = logger;
            _cosmoClient = cosmoClient;
            _eventHubOrch = eventHubProducerClient;
        }

        [Function("GetFunction")]
        public async Task<IActionResult> RunGetFunction([HttpTrigger(AuthorizationLevel.Function, "get", Route = "get-function")] HttpRequest req)
        {
            var queryParams = req.Query;
            _logger.LogInformation($"Received message: {queryParams}");

            var container = _cosmoClient.GetContainer("cdb-orch", "Container1");

            var queryString = "SELECT * FROM c WHERE c.TransactionId = '" + queryParams["id"] + "'";
            dynamic result = 0;
            using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
            {
                while (iterator.HasMoreResults)
                {
                    FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                    result = response.Resource.First();
                }
            }
            string resultado = JsonConvert.SerializeObject(result);
            return new OkObjectResult($"{resultado}");
        }

        [Function("PostFunction")]
        public async Task<IActionResult> RunPostFunction([HttpTrigger(AuthorizationLevel.Function, "post", Route = "post-function")] HttpRequest req)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            _logger.LogInformation($"Received message: {requestBody}");

            var batch = await _eventHubOrch.CreateBatchAsync();

            dynamic myObj = JsonConvert.DeserializeObject<dynamic>(requestBody);

            var objToSend = new { id = myObj.id, AccountId = myObj.AccountId, EventType = "BeginTransaction", ProductId = myObj.ProductId, Quantity = myObj.Quantity };
            
            string jsonToSend = JsonConvert.SerializeObject(objToSend);

            batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend)));

            await _eventHubOrch.SendAsync(batch);

            return new OkObjectResult($"{jsonToSend}");
        }
    }
}
