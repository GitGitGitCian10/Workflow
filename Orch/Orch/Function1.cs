using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos;
using System.Text;
using Microsoft.AspNetCore.Http;

namespace Orchestrator
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        private readonly CosmosClient _cosmosClient;

        private readonly EventHubProducerClient _eventHubPedido;
        private readonly EventHubProducerClient _eventHubInventario;
        private readonly EventHubProducerClient _eventHubPago;

        public Function1(ILogger<Function1> logger, CosmosClient cosmosClient, IDictionary<string, EventHubProducerClient> EventHubs)
        {
            _logger = logger;
            _cosmosClient = cosmosClient;
            _eventHubPedido = EventHubs["Pedido"];
            _eventHubInventario = EventHubs["Inventario"];
            _eventHubPago = EventHubs["Pago"];
        }

        [Function(nameof(Function1))]
        public async Task Run([EventHubTrigger("evh-orch", Connection = "EventHubSrc")] EventData[] events)
        {
            foreach (EventData @event in events)
            {
                string jsonString = System.Text.Encoding.UTF8.GetString(@event.EventBody.ToArray());
                _logger.LogInformation($"Received message: {jsonString}");

                dynamic jsonObject = JsonConvert.DeserializeObject(jsonString);
                var container = _cosmosClient.GetContainer("cdb-orch", "Container1");       

                switch (Convert.ToString(jsonObject.EventType))
                {
                    case "BeginTransaction":
                        var batchPedido1 = await _eventHubPedido.CreateBatchAsync();

                        var transaction = new { id = jsonObject.id, TransactionId = jsonObject.id, AccountId = jsonObject.AccountId, ProductId = jsonObject.ProductId, Quantity = jsonObject.Quantity, Status = "Pending" };
                        await container.CreateItemAsync(transaction);

                        var myObj1 = new { id = jsonObject.id, AccountId = jsonObject.AccountId, EventType = "CrearPedido", OrderId = jsonObject.id, ProductId = jsonObject.ProductId, Quantity = jsonObject.Quantity };
                        string jsonToSend1 = JsonConvert.SerializeObject(myObj1);

                        batchPedido1.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend1)));
                        
                        await _eventHubPedido.SendAsync(batchPedido1);
                        break;
                    case "SuccessCrearPedido":
                        var batchInventario1 = await _eventHubInventario.CreateBatchAsync();

                        var myObj2 = new { id = jsonObject.id, AccountId = jsonObject.AccountId, EventType = "ReservaStock", ProductId = jsonObject.ProductId, Quantity = jsonObject.Quantity };
                        string jsonToSend2 = JsonConvert.SerializeObject(myObj2);

                        batchInventario1.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend2)));

                        await _eventHubInventario.SendAsync(batchInventario1);
                        break;
                    case "ErrorCrearPedido":
                        var queryString1 = "SELECT * FROM c WHERE c.TransactionId = '" + jsonObject.id + "'";
                        dynamic result1 = 0;
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString1))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result1 = response.Resource.First();
                            }
                        }
                        result1.Status = "Cancelled";
                        await container.ReplaceItemAsync(result1, result1.id.ToString(), new PartitionKey(result1.TransactionId.ToString()));

                        break;
                    case "SuccessCancelarPedido":

                        break;
                    case "ErrorCancelarPedido":

                        break;
                    case "SuccessCompletarPedido":
                        var queryString2 = "SELECT * FROM c WHERE c.TransactionId = '" + jsonObject.id + "'";
                        dynamic result2 = 0;
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString2))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result2 = response.Resource.First();
                            }
                        }
                        result2.Status = "Completed";
                        await container.ReplaceItemAsync(result2, result2.id.ToString(), new PartitionKey(result2.TransactionId.ToString()));
                        break;
                    case "ErrorCompletarPedido":

                        break;
                    case "SuccessReservaStock":
                        var batchPago1 = await _eventHubPago.CreateBatchAsync();

                        var myObj4 = new { id = jsonObject.id, EventType = "RealizarPago", AccountId = jsonObject.AccountId, Price = jsonObject.Price };
                        string jsonToSend4 = JsonConvert.SerializeObject(myObj4);

                        batchPago1.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend4)));

                        await _eventHubPago.SendAsync(batchPago1);
                        break;
                    case "ErrorReservaStock":
                        var batchPedido2 = await _eventHubPedido.CreateBatchAsync();

                        var myObj3 = new { EventType = "CancelarPedido", OrderId = jsonObject.id };
                        string jsonToSend3 = JsonConvert.SerializeObject(myObj3);

                        batchPedido2.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend3)));

                        await _eventHubPedido.SendAsync(batchPedido2);
                        break;
                    case "SuccessUndoStock":

                        break;
                    case "SuccessRealizarPago":
                        var batchPedido3 = await _eventHubPedido.CreateBatchAsync();

                        var myObj5 = new { id = jsonObject.id, EventType = "CompletarPedido", OrderId = jsonObject.id };
                        string jsonToSend5 = JsonConvert.SerializeObject(myObj5);

                        batchPedido3.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend5)));

                        await _eventHubPedido.SendAsync(batchPedido3);
                        break;
                    case "ErrorRealizarPago":

                        break;
                    case "SuccessDeshacerPago":

                        break;
                    case "ErrorDeshacerPago":

                        break;
                }
            }
        }
    }
}