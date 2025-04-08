import time
import boto3
from django.conf import settings
from .models import TaskAssignment, Job, JobHistory

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

def do_task(data):
    return f"Task done: {data}"

def worker_loop(worker_id):
    while True:
        task = TaskAssignment.objects.filter(worker__worker_id=worker_id, status='ASSIGNED').select_related('job').first()
        if task:
            task.status = 'RUNNING'
            task.save()
            
            data = get_from_wasabi(task.job.data_location)
            result = do_task(data)
            result_location = save_to_wasabi(result, task.job.job_id)
            
            task.status = 'COMPLETED'
            task.result_location = result_location
            task.save()
            
            task.job.status = 'COMPLETED'
            task.job.result_location = result_location
            task.job.save()
            
            JobHistory.objects.create(job=task.job, worker_id=worker_id, status='COMPLETED')
            print(f"Worker {worker_id} finished job {task.job.job_id}")
        
        time.sleep(1)

if __name__ == "_main_":
    import django
    import os
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'scheduler_project.settings')
    django.setup()
    worker_loop("worker1")