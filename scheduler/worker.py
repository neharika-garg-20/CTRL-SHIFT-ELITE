from kafka import KafkaConsumer
import json
import time
import uuid
import subprocess
import requests
import boto3
from django.utils import timezone
from django.conf import settings
from scheduler.models import Worker, TaskAssignment, Job, JobHistory

# Wasabi configuration using Django settings
s3_client = boto3.client(
    "s3",
    aws_access_key_id=settings.WASABI_ACCESS_KEY,
    aws_secret_access_key=settings.WASABI_SECRET_KEY,
    endpoint_url=settings.WASABI_ENDPOINT
)

def save_to_wasabi(data, job_id, folder="results"):
    key = f"{folder}/{job_id}/result.txt"
    s3_client.put_object(Bucket=settings.WASABI_BUCKET_NAME, Key=key, Body=data.encode("utf-8"))
    return f"wasabi://{settings.WASABI_BUCKET_NAME}/{key}"

def get_from_wasabi(data_location):
    key = data_location.replace(f"wasabi://{settings.WASABI_BUCKET_NAME}/", "")
    response = s3_client.get_object(Bucket=settings.WASABI_BUCKET_NAME, Key=key)
    return response["Body"].read().decode("utf-8")

def process_file_execution(job):
    try:
        file_content = get_from_wasabi(job.data_location)
        file_type = job.data_location.split('.')[-1].lower()
        if file_type == 'py':
            process = subprocess.run(['python', '-c', file_content], capture_output=True, text=True)
        elif file_type == 'sh':
            process = subprocess.run(['bash', '-c', file_content], capture_output=True, text=True)
        else:
            return False, f"Unsupported file type: {file_type}"
        result = {'output': process.stdout} if process.returncode == 0 else {'error': process.stderr}
        return process.returncode == 0, result
    except Exception as e:
        return False, str(e)

def process_notification(job):
    try:
        config = json.loads(get_from_wasabi(job.data_location))
        message = config.get('message')
        target = config.get('target')
        method = config.get('method', 'email')
        if method == 'email':
            print(f"Sending email to {target}: {message}")  # Replace with real email logic
            return True, "Email sent"
        elif method == 'slack':
            response = requests.post(target, json={'text': message})
            response.raise_for_status()
            return True, "Slack message sent"
        else:
            return False, f"Unsupported method: {method}"
    except Exception as e:
        return False, str(e)

def process_system_automation(job):
    try:
        command = get_from_wasabi(job.data_location)
        process = subprocess.run(command, shell=True, capture_output=True, text=True)
        result = {'output': process.stdout} if process.returncode == 0 else {'error': process.stderr}
        return process.returncode == 0, result
    except Exception as e:
        return False, str(e)

def process_job(job_data):
    job = Job.objects.get(job_id=job_data['job_id'])
    job_type = job_data['job_type'].upper()

    if job_type == 'FILE_EXECUTION':
        return process_file_execution(job)
    elif job_type == 'NOTIFICATION':
        return process_notification(job)
    elif job_type == 'SYSTEM_AUTOMATION':
        return process_system_automation(job)
    else:
        return False, f"Unknown job type: {job_type}"

def send_heartbeat(worker):
    worker.last_heartbeat = timezone.now()
    worker.status = 'ACTIVE' if worker.current_load > 0 else 'IDLE'
    worker.save()

def worker_loop(worker_id):
    worker, created = Worker.objects.get_or_create(
        worker_id=worker_id,
        defaults={'hostname': 'localhost', 'status': 'IDLE', 'capacity': 5}
    )

    import threading
    def heartbeat_loop():
        while True:
            send_heartbeat(worker)
            time.sleep(5)
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    consumer = KafkaConsumer(
        'job_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='worker_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=False,
        max_poll_records=1
    )

    print(f"Worker {worker_id} starting...")
    for message in consumer:
        if worker.current_load >= worker.capacity:
            time.sleep(1)
            continue

        job_data = message.value
        job_id = job_data['job_id']
        try:
            job = Job.objects.get(job_id=job_id)
            if job.status != 'PROCESSING':
                consumer.commit()
                continue

            assignment = TaskAssignment(
                job=job,
                worker=worker,
                status='ASSIGNED',
                priority=job.priority
            )
            assignment.save()
            worker.current_load += 1
            worker.save()

            assignment.status = 'RUNNING'
            assignment.start_time = timezone.now()
            assignment.save()

            print(f"Worker {worker_id} processing job {job_id} ({job.job_type})")
            success, result = process_job(job_data)

            assignment.status = 'COMPLETED' if success else 'FAILED'
            assignment.end_time = timezone.now()
            if success and isinstance(result, dict):
                result_str = json.dumps(result)
            else:
                result_str = result
            assignment.result_location = save_to_wasabi(result_str, job_id)
            assignment.save()

            job.status = 'COMPLETED' if success else 'ERROR'
            job.last_execution_time = timezone.now()
            job.worker_id = worker.worker_id
            job.result_location = assignment.result_location
            job.save()

            JobHistory.objects.create(
                job=job,
                worker_id=worker_id,
                status='COMPLETED' if success else 'ERROR',
                duration_ms=int((assignment.end_time - assignment.start_time).total_seconds() * 1000),
                error_message=result if not success else None,
                retry_num=job.retry_count
            )

            worker.current_load -= 1
            worker.save()

            print(f"Worker {worker_id} finished job {job_id} ({assignment.status})")
            consumer.commit()

        except Job.DoesNotExist:
            print(f"Job {job_id} not found, skipping")
            consumer.commit()
        except Exception as e:
            print(f"Worker {worker_id} encountered error: {str(e)}")
            consumer.commit()

if __name__ == "__main__":
    import django
    import os
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'job_scheduler.settings')
    django.setup()
    worker_id = str(uuid.uuid4())
    worker_loop(worker_id)