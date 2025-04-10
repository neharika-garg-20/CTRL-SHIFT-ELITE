# import logging
# import os
# import sys
# import uuid
# import json
# import threading
# import time
# import subprocess
# import requests
# import boto3
# from kafka import KafkaConsumer

# # -- Django setup --
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(_file_), '..')))
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'job_scheduler.settings')
# import django
# django.setup()

# from django.utils import timezone
# from django.conf import settings
# from scheduler.models import Worker, TaskAssignment, Job, JobHistory

# # -- Logging setup --
# formatter = logging.Formatter("[%(levelname)s] %(message)s")
# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(formatter)
# logger = logging.getLogger("worker")
# logger.setLevel(logging.DEBUG)
# logger.addHandler(stream_handler)

# # -- Wasabi S3 setup --
# s3_client = boto3.client(
#     "s3",
#     aws_access_key_id=settings.WASABI_ACCESS_KEY,
#     aws_secret_access_key=settings.WASABI_SECRET_KEY,
#     endpoint_url=settings.WASABI_ENDPOINT
# )

# def save_to_wasabi(data, job_id, folder="results"):
#     key = f"{folder}/{job_id}/result.txt"
#     s3_client.put_object(Bucket=settings.WASABI_BUCKET_NAME, Key=key, Body=data.encode("utf-8"))
#     return f"wasabi://{settings.WASABI_BUCKET_NAME}/{key}"

# def get_from_wasabi(data_location):
#     key = data_location.replace(f"wasabi://{settings.WASABI_BUCKET_NAME}/", "")
#     response = s3_client.get_object(Bucket=settings.WASABI_BUCKET_NAME, Key=key)
#     return response["Body"].read().decode("utf-8")

# # -- Job processing logic --
# def process_job(job_data):
#     job = Job.objects.get(job_id=job_data['job_id'])
#     job_type = job_data['job_type'].upper()

#     def file_execution():
#         file_content = get_from_wasabi(job.data_location)
#         file_type = job.data_location.split('.')[-1].lower()
#         if file_type == 'py':
#             process = subprocess.run(['python', '-c', file_content], capture_output=True, text=True)
#         elif file_type == 'sh':
#             process = subprocess.run(['bash', '-c', file_content], capture_output=True, text=True)
#         else:
#             return False, f"Unsupported file type: {file_type}"
#         result = {'output': process.stdout} if process.returncode == 0 else {'error': process.stderr}
#         return process.returncode == 0, result

#     def notification():
#         config = json.loads(get_from_wasabi(job.data_location))
#         message = config.get('message')
#         target = config.get('target')
#         method = config.get('method', 'email')
#         if method == 'email':
#             print(f"Sending email to {target}: {message}")
#             return True, "Email sent"
#         elif method == 'slack':
#             response = requests.post(target, json={'text': message})
#             response.raise_for_status()
#             return True, "Slack message sent"
#         else:
#             return False, f"Unsupported method: {method}"

#     def system_automation():
#         command = get_from_wasabi(job.data_location)
#         process = subprocess.run(command, shell=True, capture_output=True, text=True)
#         result = {'output': process.stdout} if process.returncode == 0 else {'error': process.stderr}
#         return process.returncode == 0, result

#     handlers = {
#         'FILE_EXECUTION': file_execution,
#         'NOTIFICATION': notification,
#         'SYSTEM_AUTOMATION': system_automation
#     }

#     handler = handlers.get(job_type)
#     if handler:
#         return handler()
#     return False, f"Unknown job type: {job_type}"

# def heartbeat_loop(worker):
#     while True:
#         worker.last_heartbeat = timezone.now()
#         worker.status = 'ACTIVE' if worker.current_load > 0 else 'IDLE'
#         worker.save()
#         time.sleep(5)

# # -- Task handler --
# def handle_kq_task(job_data, worker_id):
#     job_id = job_data.get('job_id')
#     logger.info(f"📨 Received job: {job_id}")

#     try:
#         job = Job.objects.get(job_id=job_id)
#         if job.status != 'PROCESSING':
#             logger.warning(f"Job {job_id} not in PROCESSING state. Skipping.")
#             return

#         # Use the worker's own worker_id, not from job_data
#         worker_instance = Worker.objects.get(worker_id=worker_id)

#         assignment = TaskAssignment.objects.create(
#             job=job,
#             worker=worker_instance,
#             status='ASSIGNED',
#             priority=job.priority
#         )
#         worker_instance.current_load += 1
#         worker_instance.save()

#         assignment.status = 'RUNNING'
#         assignment.start_time = timezone.now()
#         assignment.save()

#         success, result = process_job(job_data)

#         assignment.status = 'COMPLETED' if success else 'FAILED'
#         assignment.end_time = timezone.now()
#         assignment.result_location = save_to_wasabi(json.dumps(result), job_id) if success else None
#         assignment.save()

#         job.status = 'COMPLETED' if success else 'ERROR'
#         job.last_execution_time = timezone.now()
#         job.worker_id = worker_instance.worker_id
#         job.result_location = assignment.result_location
#         job.save()

#         JobHistory.objects.create(
#             job=job,
#             worker_id=worker_instance.worker_id,
#             status='COMPLETED' if success else 'ERROR',
#             duration_ms=int((assignment.end_time - assignment.start_time).total_seconds() * 1000),
#             error_message=None if success else str(result),
#             retry_num=job.retry_count
#         )

#         worker_instance.current_load -= 1
#         worker_instance.save()

#         logger.info(f"✅ Job {job_id} completed with status: {assignment.status}")

#     except Job.DoesNotExist:
#         logger.error(f"❌ Job {job_id} not found")
#     except Exception as e:
#         logger.exception(f"❌ Error processing job {job_id}: {str(e)}")

# # -- Main entry point --
# if _name_ == "_main_":
#     worker_id = str(uuid.uuid4())
#     worker_obj, created = Worker.objects.get_or_create(
#         worker_id=worker_id,
#         defaults={'hostname': 'localhost', 'status': 'IDLE', 'capacity': 5}
#     )
#     if not created:
#         logger.warning(f"Worker {worker_id} already exists, using existing instance.")

#     # Kafka Consumer Setup
#     consumer = KafkaConsumer(
#         'job_topic',
#         bootstrap_servers='127.0.0.1:9092',
#         group_id='worker_group',
#         auto_offset_reset='latest',
#         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#         enable_auto_commit=False
#     )

#     # Start heartbeat in background
#     threading.Thread(target=heartbeat_loop, args=(worker_obj,), daemon=True).start()

#     logger.info(f"🚀 Worker {worker_id} is up and listening...")
#     for message in consumer:
#         job_data = message.value
#         handle_kq_task(job_data, worker_id)  # Pass worker_id explicitly
#         consumer.commit()
import logging
import os
import sys
import uuid
import json
import threading
import time
import subprocess
import requests
import boto3
from kafka import KafkaConsumer, KafkaProducer
from botocore.exceptions import ClientError
from django.core.mail import send_mail

# -- Django setup --
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'job_scheduler.settings')
import django
django.setup()

from django.utils import timezone
from django.conf import settings
from scheduler.models import Worker, TaskAssignment, Job, JobHistory

# -- Logging setup --
formatter = logging.Formatter("[%(levelname)s] %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger = logging.getLogger("worker")
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

# -- Wasabi S3 setup --
s3_client = boto3.client(
    "s3",
    aws_access_key_id=settings.WASABI_ACCESS_KEY,
    aws_secret_access_key=settings.WASABI_SECRET_KEY,
    endpoint_url=settings.WASABI_ENDPOINT
)

# -- Kafka Producer for Retries --
producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

def save_to_wasabi(data, job_id, folder="results"):
    key = f"{folder}/{job_id}/result.txt"
    try:
        s3_client.put_object(Bucket=settings.WASABI_BUCKET_NAME, Key=key, Body=data.encode("utf-8"))
        return f"wasabi://{settings.WASABI_BUCKET_NAME}/{key}"
    except Exception as e:
        logger.error(f"Failed to save to Wasabi: {str(e)}")
        return None

def get_from_wasabi(data_location):
    key = data_location.replace(f"wasabi://{settings.WASABI_BUCKET_NAME}/", "")
    logger.debug(f"Fetching from Wasabi: Bucket={settings.WASABI_BUCKET_NAME}, Key={key}")
    try:
        response = s3_client.get_object(Bucket=settings.WASABI_BUCKET_NAME, Key=key)
        return response["Body"].read().decode("utf-8")
    except ClientError as e:
        logger.error(f"Wasabi error: {str(e)}")
        raise

def enqueue_job(job):
    message = {
        'job_id': str(job.job_id),
        'job_type': job.job_type,
        'data_location': job.data_location,
        'priority': job.priority,
        'user_id': job.user.user_id,
        'retry_count': job.retry_count,
        'max_retries': job.max_retries,
        'worker_id': None
    }
    producer.send('job_topic', value=message)
    producer.flush()
    logger.info(f"📤 Requeued Job ID {job.job_id} to Kafka")

# -- Job processing logic --
def process_job(job_data):
    job = Job.objects.get(job_id=job_data['job_id'])
    job_type = job_data['job_type'].upper()

    def file_execution():
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
            return False, f"File execution failed: {str(e)}"

    def notification():
        try:
            raw_data = get_from_wasabi(job.data_location)
            logger.debug(f"Notification raw data: {raw_data}")
            try:
                method, target, message = raw_data.split(":", 2)
                config = {"method": method, "target": target, "message": message}
            except ValueError:
                logger.error(f"Invalid notification data format: {raw_data}")
                return False, f"Invalid notification data format: {raw_data}"

            logger.debug(f"Notification config: method={method}, target={target}, message={message}")
            logger.debug(f"Method value: '{method}' (length={len(method)})")  # Fixed typo

            logger.debug("Checking method condition")
            if method.strip() == 'email':  # Strip to handle whitespace
                logger.info("email method called")
                send_mail(
                    subject='Scheduled Notification',
                    message=message,
                    from_email=settings.EMAIL_HOST_USER,
                    recipient_list=[target],
                    fail_silently=False,
                )
                logger.info(f"Email sent to {target}: {message}")
                return True, "Email sent"
            elif method.strip() == 'slack':
                logger.info("slack method called")
                response = requests.post(target, json={'text': message})
                response.raise_for_status()
                logger.info(f"Slack message sent to {target}: {message}")
                return True, "Slack message sent"
            else:
                logger.debug(f"Method '{method}' not recognized as 'email' or 'slack'")
                return False, f"Unsupported method: {method}"
        except Exception as e:
            logger.error(f"Notification failed: {str(e)}", exc_info=True)
            return False, f"Notification failed: {str(e)}"

    def system_automation():
        try:
            command = get_from_wasabi(job.data_location)
            logger.debug(f"Executing system automation command: {command}")
            process = subprocess.run(command, shell=True, capture_output=True, text=True)
            result = {'output': process.stdout} if process.returncode == 0 else {'error': process.stderr}
            logger.debug(f"Command result: returncode={process.returncode}, output={process.stdout}, error={process.stderr}")
            return process.returncode == 0, result
        except Exception as e:
            logger.error(f"System automation failed: {str(e)}")
            return False, f"System automation failed: {str(e)}"

    handlers = {
        'FILE_EXECUTION': file_execution,
        'NOTIFICATION': notification,
        'SYSTEM_AUTOMATION': system_automation
    }

    handler = handlers.get(job_type)
    if handler:
        return handler()
    return False, f"Unknown job type: {job_type}"

def heartbeat_loop(worker):
    while True:
        worker.last_heartbeat = timezone.now()
        worker.status = 'ACTIVE' if worker.current_load > 0 else 'IDLE'
        worker.save()
        time.sleep(5)

# -- Task handler --
def handle_kq_task(job_data, worker_id):
    job_id = job_data.get('job_id')
    logger.info(f"📨 Received job: {job_id}")

    try:
        job = Job.objects.get(job_id=job_id)
        if job.status != 'PROCESSING':
            logger.warning(f"Job {job_id} not in PROCESSING state. Skipping.")
            return

        worker_instance = Worker.objects.get(worker_id=worker_id)

        assignment = TaskAssignment.objects.create(
            job=job,
            worker=worker_instance,
            status='ASSIGNED',
            priority=job.priority
        )
        worker_instance.current_load += 1
        worker_instance.save()

        assignment.status = 'RUNNING'
        assignment.start_time = timezone.now()
        assignment.save()

        success, result = process_job(job_data)

        assignment.status = 'COMPLETED' if success else 'FAILED'
        assignment.end_time = timezone.now()
        assignment.result_location = save_to_wasabi(json.dumps(result), job_id) if success else None
        assignment.save()

        if success:
            job.status = 'COMPLETED'
        else:
            if job.retry_count < job.max_retries:
                job.retry_count += 1
                job.status = 'PENDING'
                logger.info(f"Retrying job {job_id} ({job.retry_count}/{job.max_retries})")
                enqueue_job(job)
            else:
                job.status = 'ERROR'
                logger.error(f"Job {job_id} failed after {job.max_retries} retries")

        job.last_execution_time = timezone.now()
        job.worker_id = worker_instance.worker_id
        job.result_location = assignment.result_location if success else None
        job.save()

        JobHistory.objects.create(
            job=job,
            worker_id=worker_instance.worker_id,
            status=assignment.status,
            duration_ms=int((assignment.end_time - assignment.start_time).total_seconds() * 1000),
            error_message=None if success else str(result),
            retry_num=job.retry_count
        )

        worker_instance.current_load -= 1
        worker_instance.save()

        logger.info(f"✅ Job {job_id} completed with status: {assignment.status}")

    except Job.DoesNotExist:
        logger.error(f"❌ Job {job_id} not found")
    except Exception as e:
        logger.exception(f"❌ Error processing job {job_id}: {str(e)}")

# -- Main entry point --
if __name__ == "__main__":
    worker_id = str(uuid.uuid4())
    worker_obj, created = Worker.objects.get_or_create(
        worker_id=worker_id,
        defaults={'hostname': 'localhost', 'status': 'IDLE', 'capacity': 5}
    )
    if not created:
        logger.warning(f"Worker {worker_id} already exists, using existing instance.")

    # Kafka Consumer Setup
    consumer = KafkaConsumer(
        'job_topic',
        bootstrap_servers='127.0.0.1:9092',
        group_id='worker_group',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=False
    )

    # Start heartbeat in background
    threading.Thread(target=heartbeat_loop, args=(worker_obj,), daemon=True).start()

    logger.info(f"🚀 Worker {worker_id} is up and listening...")
    for message in consumer:
        job_data = message.value
        job_id = job_data.get('job_id')
        try:
            job = Job.objects.get(job_id=job_id)
            if job.status in ['COMPLETED', 'ERROR']:
                logger.info(f"Skipping already processed job {job_id} with status {job.status}")
                consumer.commit()
                continue
        except Job.DoesNotExist:
            logger.error(f"Job {job_id} not found, skipping")
            consumer.commit()
            continue

        handle_kq_task(job_data, worker_id)
        consumer.commit()