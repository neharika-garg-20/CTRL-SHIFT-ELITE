# job_scheduler/scheduler/serializers.py

from rest_framework import serializers
from .models import User, Job, Worker, TaskAssignment, JobHistory

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['user_id', 'username', 'api_key']

class JobSerializer(serializers.ModelSerializer):
    user = UserSerializer()  # Nested serializer for User
    class Meta:
        model = Job
        fields = [
            'job_id', 'submission_time', 'user', 'job_type', 'schedule_time', 
            'period_minutes', 'priority', 'data_location', 'status', 
            'last_execution_time', 'max_retries', 'retry_count', 'worker_id', 
            'result_location', 'is_periodic'
        ]

class WorkerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Worker
        fields = ['worker_id', 'hostname', 'status', 'last_heartbeat', 'capacity', 'current_load']

class TaskAssignmentSerializer(serializers.ModelSerializer):
    job = JobSerializer()  # Nested serializer for Job
    worker = WorkerSerializer()  # Nested serializer for Worker

    class Meta:
        model = TaskAssignment
        fields = [
            'assignment_id', 'assigned_time', 'job', 'worker', 'status', 
            'start_time', 'end_time', 'priority', 'result_location'
        ]

class JobHistorySerializer(serializers.ModelSerializer):
    job = JobSerializer()  # Nested serializer for Job

    class Meta:
        model = JobHistory
        fields = [
            'job', 'execution_time', 'worker_id', 'status', 'duration_ms', 
            'error_message', 'retry_num'
        ]
