import argparse
import atexit
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests
from flatten_json import flatten
from prometheus_client import Gauge, REGISTRY, start_http_server


PID_FILE_PREFIX = "cm_exporter"
pid_file = ""

BANNED_LABELS = (
    "metricName",
    "value",
    "startTime",
    "timestamp",
    "endTime",
    "expression",
    "type",
    "attributes_category",
    "attributes_active",
    "aggregateStatistics_maxTime",
    "aggregateStatistics_minTime",
    "aggregateStatistics_sampleTime",
)


def create_pid_file(bind_port):
    pid = os.getpid()

    global pid_file
    pid_file = f"{PID_FILE_PREFIX}_{bind_port}.pid"

    with open(pid_file, "w") as f:
        f.write(str(pid))

    atexit.register(remove_pid_file)


def remove_pid_file():
    global pid_file

    if pid_file and os.path.exists(pid_file):
        os.remove(pid_file)


def chunks(lista, tamanho):
    for i in range(0, len(lista), tamanho):
        yield lista[i:i + tamanho]


def load_metrics(filepath):
    metrics = []

    with open(filepath, "r") as metricas_file:
        for linha in metricas_file.readlines():
            linha = linha.strip()

            if not linha:
                continue

            if linha.startswith("#"):
                continue

            metrics.append(linha)

    return metrics


def filter_invalid_metrics(metrics, invalid_metrics):
    return [
        metric
        for metric in metrics
        if metric not in invalid_metrics
    ]


def extract_invalid_metric(response_text):
    match = re.search(r"Invalid metric '([^']+)'", response_text)

    if match:
        return match.group(1)

    return None


def build_time_window(lookback_minutes):
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=lookback_minutes)

    return {
        "from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def fetch_batch(session, args, metrics_batch):
    query = "SELECT " + ", ".join(metrics_batch)

    params = {
        "query": query,
        **build_time_window(args.lookback_minutes),
    }

    auth = None

    if args.cloudera_username and args.cloudera_password:
        auth = (args.cloudera_username, args.cloudera_password)

    response = session.get(
        args.cloudera_timeseries_endpoint_url,
        auth=auth,
        params=params,
        timeout=args.request_timeout,
    )

    return response


def extract_all_time_series(response_json):
    all_time_series = []

    for item in response_json.get("items", []):
        all_time_series.extend(item.get("timeSeries", []))

    return all_time_series


def final_metric_name(metric_name, metadata):
    attributes = metadata.get("attributes", {})
    category = attributes.get("category", "")

    if category:
        return f"{metric_name}_by_{category.lower()}"

    return metric_name


def collect_label_names(time_series_group):
    labels_names = []

    for its in time_series_group:
        info = its.get("metadata", {})
        info_flat = flatten(info)

        for label in info_flat.keys():
            if label not in labels_names and label not in BANNED_LABELS:
                labels_names.append(label)

        data = its.get("data", [])

        for ent in data:
            ent_flat = flatten(ent)

            for label in ent_flat.keys():
                if label not in labels_names and label not in BANNED_LABELS:
                    labels_names.append(label)

    return labels_names


def register_or_realign_metric(metric_collected, metric_name, labels_names, metric_type):
    if (
        metric_name in metric_collected
        and set(labels_names) != set(metric_collected[metric_name]._labelnames)
    ):
        old_metric = metric_collected.pop(metric_name)
        REGISTRY.unregister(old_metric)

    if metric_name not in metric_collected:
        if metric_type == "SAMPLE":
            metric_collected[metric_name] = Gauge(metric_name, "", labels_names)
        else:
            print(f"metricName {metric_name} unknown type {metric_type}", flush=True)
            return False

    return True


def process_time_series(resp, metric_collected):
    grouped = {}

    for its in resp:
        metadata = its.get("metadata", {})
        metric_name = metadata.get("metricName", "")

        if not metric_name:
            continue

        prometheus_metric_name = final_metric_name(metric_name, metadata)

        grouped.setdefault(prometheus_metric_name, []).append(its)

    for prometheus_metric_name, time_series_group in grouped.items():
        labels_names = collect_label_names(time_series_group)

        for its in time_series_group:
            metadata = its.get("metadata", {})
            info_flat = flatten(metadata)
            data = its.get("data", [])

            for ent in data:
                ent_copy = dict(ent)

                value = ent_copy.pop("value", "")

                ent_copy.update(info_flat)
                ent_flat = flatten(ent_copy)

                labels_values = [ent_flat.get(label, "") for label in labels_names]
                metric_type = ent_flat.get("type")

                registered = register_or_realign_metric(
                    metric_collected,
                    prometheus_metric_name,
                    labels_names,
                    metric_type,
                )

                if not registered:
                    continue

                try:
                    metric_collected[prometheus_metric_name].labels(*labels_values).set(value)
                except Exception as e:
                    print(f"\n{prometheus_metric_name} collecting failed. {e}", flush=True)


def collect_batch(session, args, metrics_batch, metric_collected, invalid_metrics):
    start_time = time.time()

    try:
        response = fetch_batch(session, args, metrics_batch)
    except requests.exceptions.Timeout:
        print(f"\nTIMEOUT no lote com {len(metrics_batch)} métricas: {metrics_batch}", flush=True)
        return False
    except requests.exceptions.RequestException as e:
        print(f"\nERRO HTTP no lote {metrics_batch}: {e}", flush=True)
        return False

    duration = time.time() - start_time

    if duration > args.slow_log_seconds:
        print(
            f"\nLOTE LENTO: {len(metrics_batch)} métricas demoraram {duration:.2f}s",
            flush=True,
        )

    if response.status_code != 200:
        print(f"\nHTTP {response.status_code} no lote: {metrics_batch}", flush=True)
        print(response.text[:500], flush=True)

        invalid_metric = extract_invalid_metric(response.text)

        if invalid_metric:
            print(f"\nMétrica inválida detectada: {invalid_metric}", flush=True)

            if len(metrics_batch) == 1:
                invalid_metrics.add(invalid_metric)

                print(
                    f"Métrica ignorada em memória nos próximos ciclos: {invalid_metric}",
                    flush=True,
                )

                return True

        return False

    try:
        response_json = response.json()
    except Exception as e:
        print(f"\nJSON inválido no lote {metrics_batch}: {e}", flush=True)
        return False

    resp = extract_all_time_series(response_json)

    if not resp:
        print(f"\nSem retorno no lote: {metrics_batch}", flush=True)
        return True

    process_time_series(resp, metric_collected)

    return True


def collect_with_fallback(session, args, metrics_batch, metric_collected, invalid_metrics):
    metrics_batch = [
        metric
        for metric in metrics_batch
        if metric not in invalid_metrics
    ]

    if not metrics_batch:
        return

    ok = collect_batch(session, args, metrics_batch, metric_collected, invalid_metrics)

    if ok:
        return

    if len(metrics_batch) == 1:
        metric = metrics_batch[0]
        print(f"\nMétrica falhou individualmente: {metric}", flush=True)
        return

    print(f"\nFallback individual para lote com {len(metrics_batch)} métricas", flush=True)

    for metric in metrics_batch:
        if metric in invalid_metrics:
            continue

        collect_batch(session, args, [metric], metric_collected, invalid_metrics)


parser = argparse.ArgumentParser(description="Cloudera API Timeseries Exporter")

parser.add_argument(
    "cloudera_timeseries_endpoint_url",
    type=str,
    help="Full URL for Cloudera Timeseries API Endpoint",
)
parser.add_argument("--cloudera_username", type=str, help="Username", default="")
parser.add_argument("--cloudera_password", type=str, help="Password", default="")
parser.add_argument("--port", type=int, default=8000, help="Listening port")
parser.add_argument("--addr", type=str, default="0.0.0.0", help="Binding address")
parser.add_argument("--interval", type=int, default=30, help="Seconds between collection cycles")
parser.add_argument("--metrics_list_filepath", type=str, default="./metrics", help="Metrics list filepath")
parser.add_argument("--batch_size", type=int, default=15, help="Number of metrics per Cloudera API request")
parser.add_argument("--request_timeout", type=int, default=30, help="HTTP timeout in seconds")
parser.add_argument("--lookback_minutes", type=int, default=5, help="Time window used in Cloudera API query")
parser.add_argument("--slow_log_seconds", type=float, default=5.0, help="Log batches slower than this value")

args = parser.parse_args()


if __name__ == "__main__":
    create_pid_file(args.port)

    start_http_server(addr=args.addr, port=args.port)

    metric_collected = {}
    invalid_metrics = set()

    session = requests.Session()

    while True:
        cycle_start = time.time()

        metrics = load_metrics(args.metrics_list_filepath)
        total_original_metrics = len(metrics)

        metrics = filter_invalid_metrics(metrics, invalid_metrics)

        total_valid_metrics = len(metrics)
        total_batches = 0

        for metrics_batch in chunks(metrics, args.batch_size):
            total_batches += 1
            collect_with_fallback(
                session,
                args,
                metrics_batch,
                metric_collected,
                invalid_metrics,
            )

        cycle_duration = time.time() - cycle_start

        print(
            f"\nCiclo finalizado: "
            f"{total_valid_metrics}/{total_original_metrics} métricas válidas "
            f"em {total_batches} lotes. "
            f"Ignoradas em memória: {len(invalid_metrics)} inválidas. "
            f"Duração: {cycle_duration:.2f}s",
            flush=True,
        )

        sleep_time = max(args.interval, 1)
        time.sleep(sleep_time)
