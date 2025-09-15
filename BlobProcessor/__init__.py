import logging
import os
import json
from urllib.parse import urlparse
from io import StringIO

import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential


def _parse_container_blob_from_subject(subject: str):
    """
    subject típico:
      /blobServices/default/containers/input/blobs/some/path/file.csv
    """
    parts = [p for p in subject.split('/') if p]
    # ...containers/<container>/blobs/<path...>
    try:
      ci = parts.index('containers')
      bi = parts.index('blobs')
      container = parts[ci + 1]
      blob_path = "/".join(parts[bi + 1:])
      return container, blob_path
    except Exception:
      return None, None


def _get_blob_service_client_from_env_or_mi(account_name: str) -> BlobServiceClient:
    # 1) Si hay cadena de conexión para datos, úsala (local o simple)
    conn = os.getenv("DATA_STORAGE_CONNECTION_STRING")
    if conn:
        return BlobServiceClient.from_connection_string(conn)

    # 2) Si no, usa Managed Identity (en Azure)
    account_url = f"https://{account_name}.blob.core.windows.net"
    credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    return BlobServiceClient(account_url=account_url, credential=credential)


def main(event: func.EventGridEvent):
    logging.info("Evento recibido")
    data = event.get_json()

    # Datos clave del evento
    subject = event.subject or data.get("subject")  # redundancia por si cambia el esquema
    url = data.get("url")
    if not url:
        logging.error("El evento no contiene 'url'. Payload: %s", json.dumps(data))
        return

    parsed = urlparse(url)
    account_name = parsed.netloc.split(".")[0]  # <account>.blob.core.windows.net

    # Descubre contenedor y blob (más robusto con subject)
    container, blob_name = _parse_container_blob_from_subject(subject or "")
    if not container or not blob_name:
        # alternativa: deducir del path de la URL
        path_parts = [p for p in parsed.path.split('/') if p]
        if len(path_parts) >= 2:
            container = path_parts[0]
            blob_name = "/".join(path_parts[1:])
        else:
            logging.error("No se pudo determinar container/blob del evento. subject=%s, url=%s", subject, url)
            return

    input_container = os.getenv("INPUT_CONTAINER", "input")
    output_container = os.getenv("OUTPUT_CONTAINER", "output")

    # Solo procesar si proviene del contenedor de entrada
    if container != input_container:
        logging.info("Ignorado: contenedor %s no es el de entrada %s", container, input_container)
        return

    # Validar extensión
    if not blob_name.lower().endswith(".csv"):
        logging.info("Ignorado: blob %s no es CSV", blob_name)
        return

    # Cliente de Blob
    bsc = _get_blob_service_client_from_env_or_mi(account_name)
    input_blob = bsc.get_blob_client(container=input_container, blob=blob_name)

    # Descargar CSV
    logging.info("Descargando blob: %s/%s", input_container, blob_name)
    try:
        csv_text = input_blob.download_blob().readall().decode("utf-8")
    except Exception as e:
        logging.exception("Error descargando blob: %s", e)
        return

    # Transformaciones con pandas
    try:
        df = pd.read_csv(StringIO(csv_text))
        # ✨ TRANSFORMACIONES DE EJEMPLO
        df = df.dropna(how="all")                         # elimina filas totalmente vacías
        df.columns = [c.strip().upper() for c in df.columns]  # columnas en MAYÚSCULAS
        # Añade más reglas de negocio aquí si quieres
    except Exception as e:
        logging.exception("Error procesando CSV: %s", e)
        return

    # Subir resultado al contenedor de salida
    out_name = f"processed_{os.path.basename(blob_name)}"
    out_blob = bsc.get_blob_client(container=output_container, blob=out_name)
    try:
        csv_out = df.to_csv(index=False)
        out_blob.upload_blob(csv_out, overwrite=True, content_type="text/csv; charset=utf-8")
        logging.info("Procesado OK → %s/%s", output_container, out_name)
    except Exception as e:
        logging.exception("Error subiendo resultado: %s", e)
