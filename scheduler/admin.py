from django.contrib import admin
from .models import User, Job, Worker, TaskAssignment, JobHistory

@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    list_display = ('user_id', 'username', 'api_key')
    search_fields = ('username',)

@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = ('job_id', 'user', 'job_type', 'status', 'schedule_time', 'priority', 'retry_count', 'is_periodic')
    list_filter = ('status', 'job_type', 'is_periodic')
    search_fields = ('job_id', 'user__username')

@admin.register(Worker)
class WorkerAdmin(admin.ModelAdmin):
    list_display = ('worker_id', 'hostname', 'status', 'last_heartbeat', 'capacity', 'current_load')
    list_filter = ('status',)

@admin.register(TaskAssignment)
class TaskAssignmentAdmin(admin.ModelAdmin):
    list_display = ('assignment_id', 'job', 'worker', 'status', 'priority', 'start_time', 'end_time')
    list_filter = ('status',)
    search_fields = ('job__job_id', 'worker__worker_id')

@admin.register(JobHistory)
class JobHistoryAdmin(admin.ModelAdmin):
    list_display = ('job', 'worker_id', 'status', 'execution_time', 'duration_ms', 'retry_num')
    list_filter = ('status',)

