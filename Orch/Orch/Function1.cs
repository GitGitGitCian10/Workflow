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

                string queryString = "";
                dynamic result = 0;

                switch (Convert.ToString(jsonObject.EventType))
                {
                    case "BeginTransaction": // Evento que empieza la transacción
                        var batchPedido1 = await _eventHubPedido.CreateBatchAsync();

                        var transaction = new { id = jsonObject.id, TransactionId = jsonObject.id, AccountId = jsonObject.AccountId, ProductId = jsonObject.ProductId, Quantity = jsonObject.Quantity, Status = "Pending" };
                        await container.CreateItemAsync(transaction);

                        var myObj1 = new { id = jsonObject.id, AccountId = jsonObject.AccountId, EventType = "CrearPedido", OrderId = jsonObject.id, ProductId = jsonObject.ProductId, Quantity = jsonObject.Quantity };
                        string jsonToSend1 = JsonConvert.SerializeObject(myObj1);

                        batchPedido1.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend1)));
                        
                        await _eventHubPedido.SendAsync(batchPedido1);
                        break;
                    case "SuccessCrearPedido": // Evento de reservar stock a un pedido creado
                        var batchInventario1 = await _eventHubInventario.CreateBatchAsync();

                        var myObj2 = new { id = jsonObject.id, AccountId = jsonObject.AccountId, EventType = "ReservaStock", ProductId = jsonObject.ProductId, Quantity = jsonObject.Quantity };
                        string jsonToSend2 = JsonConvert.SerializeObject(myObj2);

                        batchInventario1.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend2)));

                        await _eventHubInventario.SendAsync(batchInventario1);
                        break;
                    case "ErrorCrearPedido": // Evento de cancelar transacción debido a pedido fallido
                        queryString = "SELECT * FROM c WHERE c.TransactionId = '" + jsonObject.id + "'";
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        result.Status = "Cancelled";
                        await container.ReplaceItemAsync(result, result.id.ToString(), new PartitionKey(result.TransactionId.ToString()));

                        break;
                    case "SuccessCancelarPedido": // Evento de cancelar transacción debido a pedido cancelado
                        queryString = "SELECT * FROM c WHERE c.TransactionId = '" + jsonObject.id + "'";
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        result.Status = "Cancelled";
                        await container.ReplaceItemAsync(result, result.id.ToString(), new PartitionKey(result.TransactionId.ToString()));
                        break;
                    case "ErrorCancelarPedido": // Evento de cancelar transacción debido a cancelación de pedido fallida
                        queryString = "SELECT * FROM c WHERE c.TransactionId = '" + jsonObject.id + "'";
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        result.Status = "Cancelled";
                        await container.ReplaceItemAsync(result, result.id.ToString(), new PartitionKey(result.TransactionId.ToString()));
                        break;
                    case "SuccessCompletarPedido": // Evento de completar transacción debido a pedido completado
                        queryString = "SELECT * FROM c WHERE c.TransactionId = '" + jsonObject.id + "'";
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        result.Status = "Completed";
                        await container.ReplaceItemAsync(result, result.id.ToString(), new PartitionKey(result.TransactionId.ToString()));
                        break;
                    case "ErrorCompletarPedido": // Evento de completar transacción debido a completación de pedido fallida ????
                        queryString = "SELECT * FROM c WHERE c.TransactionId = '" + jsonObject.id + "'";
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        result.Status = "Completed";
                        await container.ReplaceItemAsync(result, result.id.ToString(), new PartitionKey(result.TransactionId.ToString()));
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

                        var myObj3 = new { id = jsonObject.id, EventType = "CancelarPedido", OrderId = jsonObject.id };
                        string jsonToSend3 = JsonConvert.SerializeObject(myObj3);

                        batchPedido2.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend3)));

                        await _eventHubPedido.SendAsync(batchPedido2);
                        break;
                    case "SuccessUndoStock":
                        var batchPedido4 = await _eventHubPedido.CreateBatchAsync();

                        var myObj6 = new { id = jsonObject.id, EventType = "CancelarPedido", OrderId = jsonObject.id };
                        string jsonToSend6 = JsonConvert.SerializeObject(myObj6);

                        batchPedido4.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend6)));

                        await _eventHubPedido.SendAsync(batchPedido4);
                        break;
                    case "SuccessRealizarPago":
                        var batchPedido3 = await _eventHubPedido.CreateBatchAsync();

                        var myObj5 = new { id = jsonObject.id, EventType = "CompletarPedido", OrderId = jsonObject.id };
                        string jsonToSend5 = JsonConvert.SerializeObject(myObj5);

                        batchPedido3.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend5)));

                        await _eventHubPedido.SendAsync(batchPedido3);
                        break;
                    case "ErrorRealizarPago":
                        queryString = "SELECT * FROM c WHERE c.TransactionId = '" + jsonObject.id + "'";
                        using (FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(queryString))
                        {
                            while (iterator.HasMoreResults)
                            {
                                FeedResponse<dynamic> response = await iterator.ReadNextAsync();
                                result = response.Resource.First();
                            }
                        }
                        var batchInventario2 = await _eventHubInventario.CreateBatchAsync();

                        var myObj7 = new { id = jsonObject.id, EventType = "UndoStock", ProductId = result.ProductId.ToString(), Quantity = result.Quantity };
                        string jsonToSend7 = JsonConvert.SerializeObject(myObj7);

                        batchInventario2.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonToSend7)));

                        await _eventHubInventario.SendAsync(batchInventario2);
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