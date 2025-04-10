import os
import sys
import time
import json
import uuid
from kafka import KafkaProducer

# Django setup
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'job_scheduler.settings')
import django
django.setup()

from django.utils import timezone
from scheduler.models import Job, Worker, TaskAssignment, JobHistory, User

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Test data to upload to Wasabi
TEST_FILE_CONTENT = """
print("Hello, this is a test job!")
"""

def upload_test_file_to_wasabi(s3_client, job_id):
    """Upload a test file to Wasabi and return its location."""
    key = f"inputs/{job_id}/test.py"
    s3_client.put_object(
        Bucket='schedular',
        Key=key,
        Body=TEST_FILE_CONTENT.encode("utf-8")
    )
    return f"wasabi://schedular/{key}"

def create_test_job(s3_client):
    """Create a test user and job in the database and enqueue it."""
    test_user_id = "test_user_1"
    user, created = User.objects.get_or_create(
        user_id=test_user_id,
        defaults={'username': 'test_user'}
    )
    if created:
        print(f"Created test user with ID: {test_user_id}")

    job_id = str(uuid.uuid4())
    data_location = upload_test_file_to_wasabi(s3_client, job_id)

    job = Job.objects.create(
        job_id=job_id,
        job_type='FILE_EXECUTION',
        schedule_time=timezone.now(),
        data_location=data_location,
        priority=1,
        max_retries=3,
        retry_count=0,
        status='PENDING',
        user=user
    )

    message = {
        'job_id': str(job.job_id),
        'job_type': job.job_type,
        'data_location': job.data_location,
        'priority': job.priority,
        'user_id': job.user.user_id,
        'retry_count': job.retry_count,
        'max_retries': job.max_retries
    }
    producer.send('job_topic', value=message)
    producer.flush()
    print(f"Enqueued test job with ID: {job_id}")
    return job_id

def check_job_status(job_id):
    """Check the status of the job and its results."""
    time.sleep(5)
    job = Job.objects.get(job_id=job_id)
    history = JobHistory.objects.filter(job=job).first()
    
    print(f"\nJob Status: {job.status}")
    print(f"Result Location: {job.result_location}")
    if history:
        print(f"History Status: {history.status}")
        print(f"Duration: {history.duration_ms} ms")
        if history.error_message:
            print(f"Error: {history.error_message}")

if __name__ == "__main__":
    import boto3
    from django.conf import settings

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=settings.WASABI_ACCESS_KEY,
        aws_secret_access_key=settings.WASABI_SECRET_KEY,
        endpoint_url="https://s3.ap-southeast-1.wasabisys.com"
    )

    job_id = create_test_job(s3_client)
    check_job_status(job_id)