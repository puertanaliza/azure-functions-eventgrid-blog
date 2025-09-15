# Azure Functions + Event Grid + Blob Storage (Python)

Procesamiento **event-driven** de CSV: al subir un archivo a `input/`, se dispara una **Azure Function** (Python) que lo transforma con `pandas` y guarda el resultado en `output/`.

## Arquitectura
- **Event source**: Azure Blob Storage (evento `BlobCreated`)
- **Event router**: Azure Event Grid
- **Compute**: Azure Functions (Consumption, Python 3.10)
- **Data**: Azure Blob Storage
## Estructura
