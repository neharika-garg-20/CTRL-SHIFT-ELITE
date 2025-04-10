# import sys
# import os
# import time
# import json
# from datetime import timedelta
# from kafka import KafkaProducer
# from kq import Queue

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'job_scheduler.settings')
# import django
# django.setup()

# from django.utils import timezone
# from scheduler.models import Job, Worker, TaskAssignment

# # Kafka Producer Setup
# producer = KafkaProducer(
#     bootstrap_servers='127.0.0.1:9092',  # Matches your lensesio/fast-data-dev setup
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#     acks='all'
# )

# # KQ Queue Setup with Producer
# queue = Queue('job_topic', producer=producer)

# # === Enqueue Job to KQ ===
# def enqueue_job(job):
#     message = {
#         'job_id': str(job.job_id),
#         'job_type': job.job_type,
#         'data_location': job.data_location,
#         'priority': job.priority,
#         'user_id': job.user.user_id,
#         'retry_count': job.retry_count,
#         'max_retries': job.max_retries
#     }
#     queue.enqueue(message, partition=job.priority % 4)

# # === Check Worker Health & Reassign Tasks ===
# def check_worker_health():
#     timeout = timezone.now() - timedelta(seconds=30)
#     inactive_workers = Worker.objects.filter(last_heartbeat__lt=timeout, status__in=['ACTIVE', 'IDLE'])

#     for worker in inactive_workers:
#         worker.status = 'DOWN'
#         worker.save()

#         assignments = TaskAssignment.objects.filter(worker=worker, status__in=['ASSIGNED', 'RUNNING'])

#         for assignment in assignments:
#             job = assignment.job
#             if job.retry_count < job.max_retries:
#                 job.retry_count += 1
#                 job.status = 'PENDING'
#                 job.worker = None
#                 job.save()
#                 assignment.delete()
#                 enqueue_job(job)
#                 print(f"🔁 Requeued job {job.job_id} (retry {job.retry_count}/{job.max_retries}) from inactive worker {worker.worker_id}")
#             else:
#                 job.status = 'ERROR'
#                 job.save()
#                 assignment.status = 'FAILED'
#                 assignment.save()
#                 print(f"❌ Job {job.job_id} exceeded max retries")

# # === Schedule Ready Jobs from DB ===
# def schedule_ready_jobs():
#     ready_jobs = Job.objects.filter(
#         status='PENDING',
#         schedule_time__lte=timezone.now(),
#         is_periodic=False
#     ).order_by('-priority', 'schedule_time')

#     for job in ready_jobs:
#         job.status = 'PROCESSING'
#         job.start_time = timezone.now()
#         job.save()

#         enqueue_job(job)
#         print(f"📤 Enqueued Job ID {job.job_id} to KQ")

# # === Schedule Periodic Jobs ===
# def schedule_periodic_jobs():
#     periodic_jobs = Job.objects.filter(
#         is_periodic=True,
#         period_minutes__isnull=False,
#         last_execution_time__isnull=False,
#         status='COMPLETED'
#     )

#     due_jobs = [
#         job for job in periodic_jobs
#         if job.last_execution_time + timedelta(minutes=job.period_minutes) <= timezone.now()
#     ]

#     for job in due_jobs:
#         new_job = Job.objects.create(
#             user=job.user,
#             job_type=job.job_type,
#             schedule_time=timezone.now(),
#             period_minutes=job.period_minutes,
#             priority=job.priority,
#             data_location=job.data_location,
#             max_retries=job.max_retries,
#             is_periodic=True
#         )

#         job.last_execution_time = timezone.now()
#         job.save()

#         enqueue_job(new_job)
#         print(f"🔁 Enqueued periodic job {new_job.job_id} to KQ")

# # === Main Scheduler Loop ===
# def controller_loop():
#     while True:
#         check_worker_health()      # Step 1: Check worker health
#         schedule_ready_jobs()      # Step 2: Schedule PENDING user-submitted jobs
#         schedule_periodic_jobs()   # Step 3: Schedule periodic jobs
#         time.sleep(10)  # Poll interval

# if __name__ == "__main__":
#     controller_loop()





import sys
import os
import time
import json
from datetime import timedelta
from kafka import KafkaProducer

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'job_scheduler.settings')
import django
django.setup()

from django.utils import timezone
from scheduler.models import Job, Worker, TaskAssignment

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

# === Enqueue Job to Kafka ===
def enqueue_job(job):
    message = {
        'job_id': str(job.job_id),
        'job_type': job.job_type,
        'data_location': job.data_location,
        'priority': job.priority,
        'user_id': job.user.user_id,
        'retry_count': job.retry_count,
        'max_retries': job.max_retries,
        'worker_id': None  # Optional, can be set by worker
    }
    producer.send('job_topic', value=message)
    producer.flush()
    print(f"📤 Enqueued Job ID {job.job_id} to Kafka")

# === Check Worker Health & Reassign Tasks ===
def check_worker_health():
    timeout = timezone.now() - timedelta(seconds=30)
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
                enqueue_job(job)
                print(f"🔁 Requeued job {job.job_id} (retry {job.retry_count}/{job.max_retries}) from inactive worker {worker.worker_id}")
            else:
                job.status = 'ERROR'
                job.save()
                assignment.status = 'FAILED'
                assignment.save()
                print(f"❌ Job {job.job_id} exceeded max retries")

# === Schedule Ready Jobs from DB ===
def schedule_ready_jobs():
    ready_jobs = Job.objects.filter(
        status='PENDING',
        schedule_time__lte=timezone.now(),
        is_periodic=False
    ).order_by('-priority', 'schedule_time')

    for job in ready_jobs:
        job.status = 'PROCESSING'
        job.start_time = timezone.now()
        job.save()

        enqueue_job(job)

# === Schedule Periodic Jobs ===
def schedule_periodic_jobs():
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

        enqueue_job(new_job)
        print(f"🔁 Enqueued periodic job {new_job.job_id} to Kafka")

# === Main Scheduler Loop ===
def controller_loop():
    while True:
        check_worker_health()
        schedule_ready_jobs()
        schedule_periodic_jobs()
        time.sleep(10)

if __name__ == "__main__":
    controller_loop()