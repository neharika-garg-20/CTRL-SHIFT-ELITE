from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny, BasePermission
from rest_framework.decorators import api_view
from scheduler.models import Job, User
from scheduler.serializers import JobSerializer
import boto3
import uuid
from django.conf import settings
from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.utils import timezone
from django.contrib import messages
import requests
from django.http import JsonResponse
from django.utils.timezone import make_aware
from datetime import datetime
from django.contrib.auth.decorators import login_required
from django.contrib.auth import logout

from django.shortcuts import render, redirect
from django.contrib import messages
from django.utils.timezone import make_aware
from datetime import datetime
import uuid
from .models import Job, User
from django.shortcuts import render, redirect
from django.contrib import messages
from django.utils.timezone import make_aware
from datetime import datetime
import uuid
from .models import Job, User  # Update path based on your structure
from django.shortcuts import render, redirect
from django.utils.timezone import make_aware
from django.contrib import messages
from .models import Job, User



import boto3
from botocore.client import Config






from botocore.config import Config



s3_client = boto3.client(
    "s3",
    aws_access_key_id=settings.WASABI_ACCESS_KEY,
    aws_secret_access_key=settings.WASABI_SECRET_KEY,
    endpoint_url=settings.WASABI_ENDPOINT
)
def logout_view(request):
    logout(request)
    return redirect('login') 
def save_to_wasabi(data, job_id, folder="jobs"):
    try:
        key = f"{folder}/{job_id}/data.txt"
        s3_client.put_object(Bucket=settings.WASABI_BUCKET_NAME, Key=key, Body=data.encode("utf-8"))
        return f"wasabi://{settings.WASABI_BUCKET_NAME}/{key}"
    except Exception as e:
        raise Exception(f"Failed to save to Wasabi: {str(e)}")

def get_from_wasabi(data_location):
    try:
        key = data_location.replace(f"wasabi://{settings.WASABI_BUCKET_NAME}/", "")
        response = s3_client.get_object(Bucket=settings.WASABI_BUCKET_NAME, Key=key)
        return response["Body"].read().decode("utf-8")
    except Exception as e:
        raise Exception(f"Failed to retrieve from Wasabi: {str(e)}")

class HasAPIKey(BasePermission):
    def has_permission(self, request, view):
        api_key = request.headers.get("X-API-Key")
        if not api_key:
            return False
        return User.objects.filter(api_key=api_key).exists()

class SubmitJobView(APIView):
    permission_classes = [AllowAny]  # You can customize permissions later

    def post(self, request):
        api_key = request.headers.get("X-API-Key")
        if not api_key:
            return Response({"error": "No API key provided"}, status=status.HTTP_401_UNAUTHORIZED)

        try:
            user = User.objects.get(api_key=api_key)
        except User.DoesNotExist:
            return Response({"error": "Invalid API key"}, status=status.HTTP_401_UNAUTHORIZED)

        data = request.data.copy()
        job_id = uuid.uuid4()
        task_data = data.get("data", "default_task")

        # Save task data to Wasabi
        try:
            data_location = save_to_wasabi(task_data, job_id)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        data["job_id"] = job_id
        data["user"] = user.id
        data["data_location"] = data_location
        data["status"] = "PENDING"
        data["job_type"] = data.get("job_type", "FILE_EXECUTION")

        serializer = JobSerializer(data=data)
        if serializer.is_valid():
            job = serializer.save()
            return Response({"job_id": str(job.job_id)}, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class JobResultView(APIView):
    permission_classes = [AllowAny]

    def get(self, request, job_id):
        api_key = request.headers.get("X-API-Key")
        if not api_key:
            return Response({"error": "No API key provided"}, status=status.HTTP_401_UNAUTHORIZED)

        try:
            user = User.objects.get(api_key=api_key)
        except User.DoesNotExist:
            return Response({"error": "Invalid API key"}, status=status.HTTP_401_UNAUTHORIZED)

        try:
            job = Job.objects.get(job_id=job_id, user=user)
        except Job.DoesNotExist:
            return Response({"error": "Job not found or not yours"}, status=status.HTTP_403_FORBIDDEN)

        if job.status in ("COMPLETED", "ERROR") and job.result_location:
            try:
                result = get_from_wasabi(job.result_location)
                return Response({"status": job.status, "result": result}, status=status.HTTP_200_OK)
            except Exception as e:
                return Response({"error": f"Failed to retrieve result: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({"status": job.status}, status=status.HTTP_200_OK)

  

def submit_job(request):
    if request.method == 'POST':
        user_id = request.session.get('user_id')
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            messages.error(request, 'User not found.')
            return redirect('login')

        job_type = request.POST.get('job_type')
        schedule_time_str = request.POST.get('schedule_time')
        priority = int(1)
        max_retries = int(10)
        is_periodic = request.POST.get("is_periodic") == "on"

        try:
            schedule_time = make_aware(datetime.strptime(schedule_time_str, "%Y-%m-%dT%H:%M"))
        except (ValueError, TypeError):
            messages.error(request, 'Invalid schedule time format. Use YYYY-MM-DDTHH:MM.')
            return render(request, '../templates/submit_job.html')

        file_data = None
        task_data = ""
        job_id = uuid.uuid4()
        bucket_name = 'schedular'

        s3_client = boto3.client(
            's3',
            endpoint_url='https://s3.ap-southeast-1.wasabisys.com',
            aws_access_key_id='AUPWZZXX4J4UVI6QPP62',
            aws_secret_access_key='Me1eSZtwzllRLNtdQBW5wgF3UfzzYrvz1ZpiRwq6',
            config=Config(signature_version='s3v4')
        )

        if job_type == "FILE_EXECUTION":
            uploaded_file = request.FILES.get("file")
            if not uploaded_file:
                messages.error(request, "Please upload a file.")
                return render(request, '../templates/submit_job.html')
            file_data = uploaded_file.read()
            task_data = file_data.decode(errors="ignore")
            file_key = f"jobs/{job_id}/{uploaded_file.name}"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=file_data
            )
            data_location = f"wasabi://{bucket_name}/{file_key}"

        elif job_type == "NOTIFICATION":
            method = request.POST.get("method")
            target = request.POST.get("target")
            message = request.POST.get("message")

            
            if not all([method, target, message]):
                messages.error(request, "Method, target, and message are required for notifications.")
                return render(request, '../templates/submit_job.html')

           
            method = method.lower()
            if method not in ['email']:
                messages.error(request, "Invalid notification method. Use 'email'.")
                return render(request, '../templates/submit_job.html')

            
            task_data = f"{method}:{target}:{message}"
            file_key = f"jobs/{job_id}/notification.txt"
            try:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=file_key,
                    Body=task_data.encode('utf-8')
                )
                data_location = f"wasabi://{bucket_name}/{file_key}"
            except Exception as e:
                messages.error(request, f"Failed to upload notification to Wasabi: {str(e)}")
                return render(request, '../templates/submit_job.html')

        elif job_type == "SYSTEM_AUTOMATION":
            task_data = request.POST.get("command")
            if not task_data:
                messages.error(request, "Command is required for system automation.")
                return render(request, '../templates/submit_job.html')
            file_key = f"jobs/{job_id}/command.txt"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=task_data.encode()
            )
            data_location = f"wasabi://{bucket_name}/{file_key}"

        else:
            messages.error(request, "Invalid job type.")
            return render(request, '../templates/submit_job.html')

        
        try:
            Job.objects.create(
                job_id=job_id,
                user=user,
                period_minutes=5,
                job_type=job_type,
                schedule_time=schedule_time,
                data_location=data_location,
                priority=priority,
                max_retries=max_retries,
                is_periodic=is_periodic
            )
            messages.success(request, "Job submitted successfully!")
            return redirect('job_list')
        except Exception as e:
            messages.error(request, f"Failed to save job to database: {str(e)}")
            return render(request, '../templates/submit_job.html')

    return render(request, '../templates/submit_job.html')

@api_view(['GET'])
def get_job_results(request, job_id):
    try:
        # Fetch the job using the job_id
        job = Job.objects.get(job_id=job_id)
        # Serialize the job data
        serializer = JobSerializer(job)
        return Response(serializer.data)
    except Job.DoesNotExist:
        return Response({"error": "Job not found"}, status=404)

def job_list_view(request):
    status = request.GET.get('status', 'all')

    if status == 'pending':
        jobs = Job.objects.filter(status__iexact='PENDING')
    elif status == 'running':
        jobs = Job.objects.filter(status__iexact='PROCESSING')
    elif status == 'completed':
        jobs = Job.objects.filter(status__iexact='COMPLETED')
    else:
        jobs = Job.objects.all()

    return render(request, 'job_list.html', {'jobs': jobs})
def job_list(request):
    user_id = request.session.get('user_id')
    if not user_id:
        messages.error(request, "You must be logged in to view your jobs.")
        return redirect('login')

    try:
        user = User.objects.get(user_id=user_id)
    except User.DoesNotExist:
        messages.error(request, "User not found.")
        return redirect('login')

    status_filter = request.GET.get('status', 'ALL').upper()
    if status_filter != 'ALL':
        jobs = Job.objects.filter(user=user, status=status_filter)
    else:
        jobs = Job.objects.filter(user=user)

    return render(request, '../templates/home.html', {'jobs': jobs})


@login_required
def add_job_view(request):
    if request.method == 'POST':
        name = request.POST.get('name')
        status = request.POST.get('status', 'PENDING').upper()
        Job.objects.create(name=name, status=status)
    return redirect('job_list')

def add_job(request):
    if request.method == "POST":
        Job.objects.create(
            name=request.POST['name'],
            status=request.POST['status']
        )
    return redirect('job_list')



def signup_view(request):
    if request.method == 'POST':
        name = request.POST.get('name')
        email = request.POST.get('email')
        password = request.POST.get('password')

        if User.objects.filter(username=email).exists():
            messages.error(request, 'Email already exists.')
        else:
            user = User.objects.create(
                user_id=str(uuid.uuid4()),
                username=email,
                api_key=password
            )
            messages.success(request, 'Account created. Please log in.')
            return redirect('login')

    return render(request, '../templates/signup.html')
  

def login_view(request):
    if request.method == 'POST':
        email = request.POST.get('email')
        password = request.POST.get('password')

        try:
            user = User.objects.get(username=email, api_key=password)
            request.session['user_id'] = str(user.user_id)
            request.session['username'] = user.username
            return redirect('job_list')
        except User.DoesNotExist:
            messages.error(request, 'Invalid credentials.')

    return render(request, '../templates/login.html')  # no need for ../templates/

