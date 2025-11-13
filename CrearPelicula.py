import boto3
import uuid
import os
import json
from datetime import datetime
from typing import List, Dict, Any
from botocore.exceptions import BotoCoreError, ClientError

LOG_PATH = "/tmp/crear_pelicula.logl"  # ruta temporal en Lambda (persistencia limitada)

def iso_now() -> str:
    return datetime.utcnow().isoformat() + "Z"

def make_log_entry(tipo: str, datos: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "tipo": tipo,
        "log_datos": {
            "timestamp": iso_now(),
            **datos
        }
    }

def print_log(entry: Dict[str, Any]) -> None:
    # imprime una sola línea JSON (CloudWatch recibirá esa línea)
    print(json.dumps(entry, ensure_ascii=False))

def append_log_file(entry: Dict[str, Any], path: str = LOG_PATH) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        # No detener la ejecución por fallo en almacenamiento local de logs; imprimir advertencia en formato estándar.
        warn = make_log_entry("ERROR", {
            "action": "append_log_file",
            "message": "failed to append log file",
            "error": f"could not write to {path}"
        })
        print(json.dumps(warn, ensure_ascii=False))

# --- utilidades para consultas sobre el archivo de logs (JSON Lines) ---
def load_logs(path: str = LOG_PATH) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    entries = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                # ignorar líneas corruptas
                err = make_log_entry("ERROR", {
                    "action": "load_logs",
                    "message": f"invalid json on line {line_no} of {path}"
                })
                print_log(err)
    return entries

def filter_by_tipo(entries: List[Dict[str, Any]], tipo: str) -> List[Dict[str, Any]]:
    return [e for e in entries if e.get("tipo") == tipo]

def count_by_tipo(entries: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for e in entries:
        t = e.get("tipo", "UNKNOWN")
        counts[t] = counts.get(t, 0) + 1
    return counts

# --- Lambda handler ---
def lambda_handler(event, context):
    # Normalizar input: en APIs proxied event puede venir como event['body'] = JSON-string
    try:
        body = event.get("body", {})
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                # body es string no-json -> usar como dict vacío para que falle luego con mensaje claro
                body = {}
    except Exception:
        body = {}

    try:
        tenant_id = body['tenant_id']
        pelicula_datos = body['pelicula_datos']
    except KeyError as e:
        entry = make_log_entry("ERROR", {
            "action": "parse_input",
            "message": "missing required field in body",
            "missing_field": str(e),
            "body": body
        })
        print_log(entry)
        append_log_file(entry)
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': f"missing required field: {e}"
            }, ensure_ascii=False)
        }

    nombre_tabla = os.environ.get("TABLE_NAME")
    if not nombre_tabla:
        entry = make_log_entry("ERROR", {
            "action": "env_check",
            "message": "TABLE_NAME not set in environment"
        })
        print_log(entry)
        append_log_file(entry)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': "server configuration error: TABLE_NAME missing"}, ensure_ascii=False)
        }

    uuidv4 = str(uuid.uuid4())
    pelicula = {
        'tenant_id': tenant_id,
        'uuid': uuidv4,
        'pelicula_datos': pelicula_datos
    }

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)

    try:
        response = table.put_item(Item=pelicula)
        entry = make_log_entry("INFO", {
            "action": "create_movie",
            "status": "success",
            "pelicula": pelicula,
            "dynamodb_response_metadata": getattr(response, "get", lambda k, d=None: None)("ResponseMetadata", None)
        })
        print_log(entry)
        append_log_file(entry)
        # respuesta compatible con integración Lambda proxy (ajusta según tu API Gateway)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'pelicula': pelicula,
                'dynamodb_response': response
            }, default=str, ensure_ascii=False)
        }

    except (BotoCoreError, ClientError) as be:
        entry = make_log_entry("ERROR", {
            "action": "create_movie",
            "status": "dynamodb_failed",
            "error_message": str(be),
            "pelicula": pelicula
        })
        print_log(entry)
        append_log_file(entry)
        return {
            'statusCode': 502,
            'body': json.dumps({'error': "dynamodb error", 'details': str(be)}, ensure_ascii=False)
        }
    except Exception as e:
        entry = make_log_entry("ERROR", {
            "action": "create_movie",
            "status": "failed_unexpected",
            "error_type": e.__class__.__name__,
            "error_message": str(e),
            "pelicula": pelicula
        })
        print_log(entry)
        append_log_file(entry)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': "internal server error", 'details': str(e)}, ensure_ascii=False)
        }

# --- Ejemplos de uso de queries (ejecutar fuera del handler o en pruebas) ---
# entries = load_logs()
# print(count_by_tipo(entries))
# print([e for e in filter_by_tipo(entries, "ERROR")][-5:])  # últimos 5 errores
