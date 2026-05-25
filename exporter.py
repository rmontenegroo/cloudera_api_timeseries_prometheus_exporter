import requests
import time
from flatten_json import flatten
from prometheus_client import start_http_server, Gauge
import argparse

BANNED_LABELS = ('metricName', 'value', 'startTime', 'timestamp', 'endTime', 'expression', 'type', 'attributes_category', 'attributes_active', 'aggregateStatistics_maxTime', 'aggregateStatistics_minTime', 'aggregateStatistics_sampleTime')

parser = argparse.ArgumentParser(description="Cloudera API Timeseries Exporter")

parser.add_argument("cloudera_timeseries_endpoint_url", type=str, help="Full URL for Cloudera Timeseries API Endpoint")
parser.add_argument("--cloudera_username", type=str, help="Username", default="")
parser.add_argument("--cloudera_password", type=str, help="Password", default="")
parser.add_argument("--port", type=int, default=8000, help="Listening port")
parser.add_argument("--addr", type=str, default="0.0.0.0", help="Binding address")
parser.add_argument("--interval", type=int, default="30", help="Seconds between scrapes")
parser.add_argument("--metrics_list_filepath", type=str, default="./metrics", help="Metrics list filepath")

args = parser.parse_args()

if __name__ == "__main__":

    start_http_server(addr=args.addr, port=args.port)
    
    metric_collected = {}

    while True:
                
        with open(args.metrics_list_filepath, 'r') as metricas_file:

            for metrica in metricas_file.readlines():

                labels_names = []

                metrica = metrica.strip()

                if metrica[0] == '#':
                    continue

                params = { "query": f"SELECT { metrica }" }

                if args.cloudera_username and args.cloudera_password:
                    response = requests.get(args.cloudera_timeseries_endpoint_url, auth=(args.cloudera_username, args.cloudera_password), params=params)
                else:
                    response = requests.get(args.cloudera_timeseries_endpoint_url, params=params)

                if response.status_code == 200:

                    resp = response.json()
                    resp = resp.get('items', [None])[0].get('timeSeries', [])

                    metricName = ''

                    for its in resp:

                        info = its.get('metadata', {})

                        metricName = info.get('metricName')

                        info = flatten(info)

                        data = its.get('data', {})

                        labels_names += [l for l in list(info.keys()) if (l not in labels_names) and (l not in BANNED_LABELS)]

                        for ent in data: 
                            labels_names += [l for l in list(flatten(ent).keys()) if (l not in labels_names) and (l not in BANNED_LABELS)]

                    for i in range(len(resp)):

                        its = resp[i]

                        info = its.get('metadata', {})

                        attributes = info.get('attributes', {})

                        category = attributes.get('category', '')


                        info = flatten(info)
                        
                        data = its.get('data', {})

                        for j in range(len(data)):

                            ent = data[j]

                            value = ent.pop('value', '')

                            ent.update(info)

                            ent = flatten(ent)

                            labels_values = [ent.get(l, '') for l in labels_names]
                        
                            if category:
                                _metricName = f"{metricName}_by_{category.lower()}"

                            if not _metricName in metric_collected:
                            
                                metric_type = ent.get("type")

                                if metric_type == 'SAMPLE':
                                    metric_collected[_metricName] = Gauge(_metricName, '', labels_names)
                                else:
                                    print(f'metricName {_metricName} unknown type {ent.get("type")}')

                            try:
                                metric_collected[_metricName].labels(*labels_values).set(value)
                            except Exception as e:
                                print(f"\n{metricName} collecting failed. {e}")

        print('.', end='', flush=True)
        time.sleep(args.interval)

