using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace StrivoIngestPublish;

/// <summary>
/// Timer-triggered Azure Function that reads CSV files from a blob container
/// and publishes each row as a message to a storage queue, with a random delay
/// between 0.1 s and 3.0 s between rows.
/// </summary>
public class CsvIngestFunction
{
    private readonly BlobServiceClient _blobServiceClient;
    private readonly QueueServiceClient _queueServiceClient;
    private readonly string _blobContainerName;
    private readonly string _queueName;
    private readonly ILogger<CsvIngestFunction> _logger;

    public CsvIngestFunction(
        BlobServiceClient blobServiceClient,
        QueueServiceClient queueServiceClient,
        IConfiguration configuration,
        ILogger<CsvIngestFunction> logger)
    {
        _blobServiceClient = blobServiceClient;
        _queueServiceClient = queueServiceClient;
        _blobContainerName = configuration["BlobContainerName"] ?? "datasource";
        _queueName = configuration["QueueName"] ?? "consumethis";
        _logger = logger;
    }

    // Runs every 5 minutes by default. Override with the TimerSchedule app setting.
    [Function(nameof(CsvIngestFunction))]
    public async Task Run(
        [TimerTrigger("%TimerSchedule%")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("CsvIngestFunction triggered at {Time}", DateTime.UtcNow);

        var containerClient = _blobServiceClient.GetBlobContainerClient(_blobContainerName);
        var queueClient = _queueServiceClient.GetQueueClient(_queueName);

        await foreach (var blobItem in containerClient.GetBlobsAsync(cancellationToken: cancellationToken))
        {
            if (!blobItem.Name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            _logger.LogInformation("Processing blob: {BlobName}", blobItem.Name);

            var blobClient = containerClient.GetBlobClient(blobItem.Name);
            using var stream = await blobClient.OpenReadAsync(cancellationToken: cancellationToken);
            using var reader = new StreamReader(stream);

            int rowCount = 0;
            string? line;
            while ((line = await reader.ReadLineAsync(cancellationToken)) is not null)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                rowCount++;
                var message = CsvMessageBuilder.BuildMessage(line, blobItem.Name);

                await queueClient.SendMessageAsync(message, cancellationToken: cancellationToken);
                _logger.LogInformation("Read and published row {RowNumber} from {BlobName} to queue.", rowCount, blobItem.Name);

                int delayMs = Random.Shared.Next(100, 3001);
                await Task.Delay(delayMs, cancellationToken);
            }

            if (rowCount == 0)
            {
                _logger.LogWarning("Blob {BlobName} is empty – skipping.", blobItem.Name);
            }
            else
            {
                _logger.LogInformation("Finished {BlobName}: {RowCount} row(s) published to queue.", blobItem.Name, rowCount);
            }
        }

        _logger.LogInformation("CsvIngestFunction completed at {Time}", DateTime.UtcNow);
    }
}
