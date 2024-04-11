import json
import io
from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer
from minio import Minio

# Kafka settings
bootstrap_servers = 'observai-kafka-external-bootstrap-observability-kafka.apps.zagaobservability.zagaopensource.com:443'
topic = 'observai_main_metrics_1'
kafka_ssl_cafile = "./kafka_certs/observai.pem"
kafka_ssl_certfile = "./kafka_certs/observai.crt"

# MinIO settings
minio_endpoint = 'localhost:9000'
minio_access_key = 'metricsdata'
minio_secret_key = 'metricsdata'
minio_secure = False
minio_bucket = 'metrics-data'
minio_client = Minio(minio_endpoint,
                     access_key=minio_access_key,
                     secret_key=minio_secret_key,
                     secure=minio_secure)

def extract_data(json_data):
    output_list = []

    for resource_metric in json_data.get('resourceMetrics', []):
        output_dict = {}
        for scope_metric in resource_metric.get('scopeMetrics', []):
            for metric in scope_metric.get('metrics', []):
                if metric.get('name') == "jvm.cpu.recent_utilization":
                    for data_point in metric.get('gauge', {}).get('dataPoints', []):
                        as_double_value = data_point.get('asDouble')
                        if as_double_value is not None:
                            output_dict["cpuUsage"] = as_double_value
                            
                    for data_point in metric.get('gauge', {}).get('dataPoints', []):
                        startTimeUnixNano_value = data_point.get('startTimeUnixNano')
                        if startTimeUnixNano_value is not None:
                            output_dict["date"] = unix_nano_to_date(startTimeUnixNano_value)

                if metric.get('name') == "jvm.memory.used":
                    for data_point in metric.get('sum', {}).get('dataPoints', []):
                        as_int_value = data_point.get('asInt')
                        if as_int_value is not None:
                            output_dict["memoryUsage"] = round(int(as_int_value) / (1024 * 1024))

        for attribute in resource_metric['resource']['attributes']:
            if attribute.get('key') == 'telemetry.sdk.language':
                if 'stringValue' in attribute['value']:
                    output_dict["language"] = attribute['value']['stringValue']

        for resource_attribute in resource_metric.get('resource', {}).get('attributes', []):
            if resource_attribute.get('key') == "service.name":
                output_dict["serviceName"] = resource_attribute.get('value', {}).get('stringValue')
        
        output_list.append(output_dict)
    
    return output_list

def unix_nano_to_date(start_time_unix_nano):
    observed_time_seconds = int(start_time_unix_nano) / 1_000_000_000
    dt = datetime.fromtimestamp(observed_time_seconds, timezone.utc)
    ist_zone = timezone(timedelta(hours=5, minutes=30)) 
    ist_datetime = dt.astimezone(ist_zone)
    return ist_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def upload_to_minio(data, bucket, client, batch_number, item_index):
    filename_date = data.get("date", "unknown_date")
    filename = f"data_batch_{batch_number}_{item_index}_{filename_date}.json"
    file_data = json.dumps(data).encode('utf-8')
    client.put_object(bucket, filename, io.BytesIO(file_data), len(file_data))
    print(f"File '{filename}' uploaded successfully to bucket '{bucket}' on Minio.")

def consume_and_process_messages():
    batch_interval = 30
    last_batch_time = datetime.now()
    batch_number = 1
    item_index = 1

    consumer = KafkaConsumer(topic,
                             bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='earliest',
                             group_id='test_group',
                             security_protocol="SSL",
                             ssl_cafile=kafka_ssl_cafile,
                             ssl_certfile=kafka_ssl_certfile)

    try:
        for message in consumer:
            json_data = json.loads(message.value)
            output_list = extract_data(json_data)

            for item in output_list:
                upload_to_minio(item, minio_bucket, minio_client, batch_number, item_index)
                item_index += 1

            current_time = datetime.now()
            if (current_time - last_batch_time).total_seconds() >= batch_interval:
                batch_number += 1
                item_index = 1
                last_batch_time = current_time
    except KeyboardInterrupt:
        print("Execution interrupted by the user. Exiting...")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    consume_and_process_messages()
