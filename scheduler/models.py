from django.db import models
import uuid
from uuid import uuid4

class User(models.Model):
    user_id = models.CharField(max_length=50, primary_key=True)
    username = models.CharField(max_length=100)
    api_key = models.CharField(max_length=100, unique=True)
    def save(self, *args, **kwargs):
        if not self.api_key:
            self.api_key = uuid4().hex
        super().save(*args, **kwargs)

    def _str_(self):
        return self.username

class Job(models.Model):
    STATUS_CHOICES = (
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('ERROR', 'Error'),
    )
    job_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    submission_time = models.DateTimeField(auto_now_add=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    job_type = models.CharField(max_length=50)
    schedule_time = models.DateTimeField()
    period_minutes = models.IntegerField(null=True, blank=True)
    priority = models.IntegerField(default=0)
    data_location = models.TextField()  # Path to file in Wasabi, e.g., "wasabi://job-scheduler-bucket/jobs/<job_id>/data.txt"
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    last_execution_time = models.DateTimeField(null=True, blank=True)
    max_retries = models.IntegerField(default=3)
    retry_count = models.IntegerField(default=0)
    worker_id = models.CharField(max_length=50, null=True, blank=True)
    result_location = models.TextField(null=True, blank=True)  # Path to result in Wasabi
    is_periodic = models.BooleanField(default=False)

    def _str_(self):
        return f"Job {self.job_id}"

class Worker(models.Model):
    STATUS_CHOICES = (
        ('ACTIVE', 'Active'),
        ('IDLE', 'Idle'),
        ('DOWN', 'Down'),
    )
    worker_id = models.CharField(max_length=50, primary_key=True)
    hostname = models.CharField(max_length=100)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    last_heartbeat = models.DateTimeField(auto_now=True)
    capacity = models.IntegerField()
    current_load = models.IntegerField(default=0)

    def _str_(self):
        return self.worker_id

class TaskAssignment(models.Model):
    STATUS_CHOICES = (
        ('ASSIGNED', 'Assigned'),
        ('RUNNING', 'Running'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    )
    assignment_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    assigned_time = models.DateTimeField(auto_now_add=True)
    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    worker = models.ForeignKey(Worker, on_delete=models.CASCADE)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    start_time = models.DateTimeField(null=True, blank=True)
    end_time = models.DateTimeField(null=True, blank=True)
    priority = models.IntegerField()
    result_location = models.TextField(null=True, blank=True)

class JobHistory(models.Model):
    STATUS_CHOICES = (
        ('COMPLETED', 'Completed'),
        ('ERROR', 'Error'),
    )
    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    execution_time = models.DateTimeField(auto_now_add=True)
    worker_id = models.CharField(max_length=50)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    duration_ms = models.IntegerField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    retry_num = models.IntegerField(default=0)