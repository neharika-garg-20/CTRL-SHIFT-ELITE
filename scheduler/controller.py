import sys
import os
from django.db.models.functions import Now
from django.db.models import DurationField
from datetime import timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'job_scheduler.settings')
import django
django.setup()

from kafka import KafkaProducer
import json
import time
from django.utils import timezone
from scheduler.models import Job, Worker, TaskAssignment
from django.db.models import F, ExpressionWrapper, DateTimeField

def get_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )

def publish_job_to_topic(job, producer):
    message = {
        'job_id': str(job.job_id),
        'job_type': job.job_type,
        'data_location': job.data_location,
        'priority': job.priority,
        'user_id': job.user.user_id,
        'retry_count': job.retry_count,
        'max_retries': job.max_retries
    }
    producer.send('job_topic', value=message, partition=job.priority % 4)
    producer.flush()

def check_worker_health(producer):
    timeout = timezone.now() - timezone.timedelta(seconds=30)
    inactive_workers = Worker.objects.filter(last_heartbeat__lt=timeout, status__in=['ACTIVE', 'IDLE'])
    for worker in inactive_workers:
        worker.status = 'DOWN'
        worker.save()
        assignments = TaskAssignment.objects.filter(worker=worker, status__in=['ASSIGNED', 'RUNNING'])
        for assignment in assignments:
            job = assignment.job
            if job.retry_count < job.max_retries:
                job.retry_count += 1
                job.status = 'PENDING'
                job.worker_id = None
                job.save()
                assignment.delete()
                publish_job_to_topic(job, producer)
                print(f"Requeued job {job.job_id} (retry {job.retry_count}/{job.max_retries}) from inactive worker {worker.worker_id}")
            else:
                job.status = 'ERROR'
                job.save()
                assignment.status = 'FAILED'
                assignment.save()
                print(f"Job {job.job_id} exceeded max retries")

from datetime import timedelta
from django.utils import timezone
from scheduler.models import Job

def controller_loop():
    producer = get_producer()  

    while True:
        check_worker_health(producer)  

       
        ready_jobs = Job.objects.filter(
            status='PENDING',
            schedule_time__lte=timezone.now(),
            is_periodic=False
        ).order_by('priority', 'schedule_time')

        for job in ready_jobs:
            job.status = 'PROCESSING'
            job.start_time = timezone.now()
            job.save()

            publish_job_to_topic(job, producer) 
            print(f"Published Job ID {job.job_id} to Kafka")

        
        periodic_jobs = Job.objects.filter(
            is_periodic=True,
            period_minutes__isnull=False,
            last_execution_time__isnull=False,
            status='COMPLETED'
        )

        due_jobs = [
            job for job in periodic_jobs
            if job.last_execution_time + timedelta(minutes=job.period_minutes) <= timezone.now()
        ]

        for job in due_jobs:
            new_job = Job.objects.create(
                user=job.user,
                job_type=job.job_type,
                schedule_time=timezone.now(),
                period_minutes=job.period_minutes,
                priority=job.priority,
                data_location=job.data_location,
                max_retries=job.max_retries,
                is_periodic=True
            )
            job.last_execution_time = timezone.now()
            job.save()

            publish_job_to_topic(new_job, producer)  
            print(f"Published periodic job {new_job.job_id} to Kafka")

        time.sleep(10)


if __name__ == "__main__":
    controller_loop()





