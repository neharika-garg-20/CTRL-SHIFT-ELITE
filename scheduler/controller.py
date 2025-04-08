import time
import uuid
from django.utils import timezone
from .models import Job, Worker, TaskAssignment

def assign_job(job, worker):
    assignment = TaskAssignment(job=job, worker=worker, status='ASSIGNED', priority=job.priority)
    assignment.save()
    job.status = 'PROCESSING'
    job.worker_id = worker.worker_id
    job.save()

def controller_loop():
    Worker.objects.get_or_create(worker_id='worker1', defaults={'hostname': 'localhost', 'status': 'ACTIVE', 'capacity': 10})
    
    while True:
        jobs = Job.objects.filter(status='PENDING', schedule_time__lte=timezone.now()).order_by('schedule_time', '-priority')[:10]
        workers = Worker.objects.filter(status='ACTIVE', current_load__lt=models.F('capacity'))
        
        for job in jobs:
            if workers.exists():
                worker = workers.first()
                assign_job(job, worker)
                print(f"Assigned job {job.job_id} to worker {worker.worker_id}")
        
        time.sleep(1)

if __name__ == "_main_":
    import django
    import os
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'scheduler_project.settings')
    django.setup()
    from django.db import models
    controller_loop()