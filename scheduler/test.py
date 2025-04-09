from django.utils import timezone
from scheduler.models import User, Job

if __name__ == "__main__":
    import django
    import os
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'scheduler_project.settings')
    django.setup()

    user, _ = User.objects.get_or_create(
        user_id='test_user',
        defaults={'username': 'testuser', 'api_key': 'test_api_key'}
    )

    Job.objects.create(
        user=user,
        job_type='FILE_EXECUTION',
        schedule_time=timezone.now(),
        priority=5,
        data_location='wasabi://job-scheduler-bucket/jobs/test1/script.py'
    )
    Job.objects.create(
        user=user,
        job_type='NOTIFICATION',
        schedule_time=timezone.now(),
        priority=3,
        data_location='wasabi://job-scheduler-bucket/jobs/test2/config.json'
    )
    Job.objects.create(
        user=user,
        job_type='SYSTEM_AUTOMATION',
        schedule_time=timezone.now(),
        priority=4,
        data_location='wasabi://job-scheduler-bucket/jobs/test3/command.sh'
    )
    print("Created 3 test jobs")