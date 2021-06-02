using System;
using System.Text;
using System.Text.Json;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace EventHubTelemetrySender
{
    public static class Function1
    {
        private const string connectionString = "<EVENT HUBS NAMESPACE - CONNECTION STRING>";
        private const string eventHubName = "<EVENT HUB NAME>";

        [FunctionName("Function1")]
        public static async Task Run([TimerTrigger("0 */1 * * * *")]TimerInfo myTimer, ILogger log)
        {
            double avgTemperature = 25.0D;
            double avgCarbonDioxide = 0.00038D;
            var rand = new Random();

            //Create Batch of Events
            await using (var producerClient = new EventHubProducerClient(connectionString, eventHubName))
            {
                double currentTemperature = avgTemperature + rand.NextDouble() * 4-3;  // Unit is Celcious
                double currentCarbonDioxide = avgCarbonDioxide + rand.NextDouble() * 0.0002 - 0.0001;  //Unit is ppm
                String currentTime = $"{DateTime.UtcNow.ToString("u")}";
                String deviceID1 = $"room1_sensor1";
                String deviceID2 = $"room1_sensor2";
                String dataSchema = $"ContosoEnvSensor";

                var telemetryDataPoint1 = new
                {
                    Timestamp = currentTime,
                    DeviceId = deviceID1,
                    DataSchema = dataSchema,
                    Temperature = currentTemperature,
                    CarbonDioxide = currentCarbonDioxide
                };
                currentTemperature = avgTemperature + rand.NextDouble() * 4 - 3;  // Unit is Celcious
                currentCarbonDioxide = avgCarbonDioxide + rand.NextDouble() * 0.0002 - 0.0001;  //Unit is ppm

                var telemetryDataPoint2 = new
                {
                    Timestamp = currentTime,
                    DeviceId = deviceID2,
                    DataSchema = dataSchema,
                    Temperature = currentTemperature,
                    CarbonDioxide = currentCarbonDioxide
                };


                var messageString = JsonSerializer.Serialize(telemetryDataPoint1);

                // Create a batch of events 
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

                // Room1_Sensor1    Add Events to Batch
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(messageString)));

                // Geenrate telemetry data set for Room1_Sensor2
                messageString = JsonSerializer.Serialize(telemetryDataPoint2);
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(messageString)));

                // Send Events to Event Hub
                await producerClient.SendAsync(eventBatch);

                //Output to Console Log 
                log.LogInformation($"Room1_Sensor2:  Send telemetry to Event Hub at {DateTime.UtcNow.ToString("u")}");
                log.LogInformation($"JSON Message: {messageString}");
            }
            //log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
        }
    }
}
