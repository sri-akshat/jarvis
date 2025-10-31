# Messaging Ingestion Pipeline

The `jarvis` package now provides a simple pipeline for fetching messages from different providers and persisting them into a shared data store. The first implementation targets Gmail but the abstraction can be extended to additional providers like WhatsApp.

## Components

- **Models (`jarvis.messaging.models`)** – Provider-agnostic dataclasses describing messages and attachments.
- **Services (`jarvis.messaging.services`)** – Concrete implementations that know how to talk to each provider and return `Message` objects.
- **Pipelines (`jarvis.messaging.pipelines`)** – Orchestration helpers that stitch a service and a datastore together.
- **Datastores (`jarvis.messaging.datastore`)** – Persist the normalized messages. An SQLite implementation is provided out of the box.

## Gmail Usage

1. Create an OAuth client in the [Google Cloud Console](https://console.cloud.google.com/apis/credentials) and download the `credentials.json` file.
2. Install dependencies from `requirements.txt`.
3. Run the ingestion script with the desired Gmail search query. The example below pulls emails containing the phrase `"Meera Dixit medical reports"` and persists them to `data/messages.db`:

```bash
python fetch_gmail_messages.py "Meera Dixit medical reports" --credentials path/to/credentials.json
```

At first run an OAuth consent flow opens in the browser; subsequent executions reuse the stored token file.

Attachments are stored as binary blobs inside the `attachments` table, while message metadata lives in the `messages` table.

## Extending to WhatsApp

To add WhatsApp support, implement a new service under `jarvis.messaging.services` that subclasses `MessageService` and emits `Message` objects just like the Gmail integration. The WhatsApp API (Business API or Cloud API) can be used to fetch messages and media. Once implemented, the existing pipeline and datastore can be reused:

```python
from jarvis.messaging.pipelines.email_pipeline import ingest_messages
from jarvis.messaging.services.whatsapp_service import WhatsAppService
from jarvis.messaging.datastore import SQLiteDataStore

service = WhatsAppService(token="...", phone_number_id="...")
datastore = SQLiteDataStore(Path("data/messages.db"))
ingest_messages(service, datastore, query="from:+1234567890")
```

Because the pipeline is provider-agnostic, Gmail and WhatsApp messages share the same storage schema and downstream consumers can analyze them uniformly.
