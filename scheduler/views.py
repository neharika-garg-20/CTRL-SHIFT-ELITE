from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from scheduler.models import Job, User
from scheduler.serializers import JobSerializer
import boto3
import uuid
from django.conf import settings
# job_scheduler/scheduler/views.py

from django.shortcuts import render, redirect
from .models import Job
from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.utils import timezone
from .models import Job, User
import uuid
from django.shortcuts import render, redirect
from django.http import HttpResponse
from .models import Job, User
from django.shortcuts import render, redirect
from django.http import HttpResponse
from .models import Job, User
from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import JobSerializer
 
# Initialize Wasabi S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=settings.WASABI_ACCESS_KEY,
    aws_secret_access_key=settings.WASABI_SECRET_KEY,
    endpoint_url=settings.WASABI_ENDPOINT
)

def save_to_wasabi(data, job_id, folder="jobs"):
    key = f"{folder}/{job_id}/data.txt"
    s3_client.put_object(Bucket=settings.WASABI_BUCKET_NAME, Key=key, Body=data.encode("utf-8"))
    return f"wasabi://{settings.WASABI_BUCKET_NAME}/{key}"

def get_from_wasabi(data_location):
    key = data_location.replace(f"wasabi://{settings.WASABI_BUCKET_NAME}/", "")
    response = s3_client.get_object(Bucket=settings.WASABI_BUCKET_NAME, Key=key)
    return response["Body"].read().decode("utf-8")

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
        data_location = save_to_wasabi(task_data, job_id)
        data["job_id"] = job_id
        data["user"] = user.id
        data["data_location"] = data_location
        data["status"] = "PENDING"

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
            result = get_from_wasabi(job.result_location)
            return Response({"status": job.status, "result": result}, status=status.HTTP_200_OK)

        return Response({"status": job.status}, status=status.HTTP_200_OK)




# def submit_job(request):
#     if not request.user.is_authenticated:
#         return redirect('login')  # Redirect to login if the user is not authenticated

#     if request.method == "POST":
#         job_type = request.POST['job_type']
#         schedule_time = request.POST['schedule_time']
#         data_location = request.POST['data_location']
#         priority = request.POST['priority']
#         max_retries = request.POST['max_retries']
        
#         # Make sure request.user is a proper User instance
#         user = request.user

#         # You could also explicitly retrieve the User instance like this (just in case)
#         user_instance = User.objects.get(username=user.username)

#         # Create a new job
#         job = Job.objects.create(
#             job_type=job_type,
#             schedule_time=schedule_time,
#             data_location=data_location,
#             priority=priority,
#             max_retries=max_retries,
#             user=user_instance  # Ensure the user instance is assigned correctly
#         )
        
#         return redirect('job_list')  # Redirect to a job listing page or job detail page

#     return render(request, '../job_scheduler/submit_job.html')

from django.shortcuts import render, redirect
from scheduler.models import Job, User
from django.utils import timezone
from django.contrib import messages
import uuid

def submit_job(request):
    if request.method == 'POST':
        user_id = request.session.get('user_id')
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            messages.error(request, 'User not found.')
            return redirect('login')

        job_type = request.POST.get('job_type')
        schedule_time = request.POST.get('schedule_time')
        data_location = request.POST.get('data_location')
        priority = request.POST.get('priority')
        max_retries = request.POST.get('max_retries')

        job = Job.objects.create(
            user=user,
            job_type=job_type,
            schedule_time=schedule_time,
            data_location=data_location,
            priority=priority,
            max_retries=max_retries
        )
        messages.success(request, 'Job submitted successfully!')
        return redirect('job_list')

    return render(request, '../templates/submit_job.html')



# job_scheduler/scheduler/views.py

from rest_framework.response import Response
from rest_framework.decorators import api_view
from .models import Job
from .serializers import JobSerializer

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
# views.py
from django.shortcuts import render
from .models import Job

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
    status = request.GET.get('status')
    if status and status != "all":
        jobs = Job.objects.filter(status=status)
    else:
        jobs = Job.objects.all()
    return render(request, '../templates/home.html', {'jobs': jobs})

def add_job(request):
    if request.method == "POST":
        Job.objects.create(
            name=request.POST['name'],
            status=request.POST['status']
        )
    return redirect('job_list')



def submit_job_form(request):
    if request.method == "POST":
        api_key = request.POST.get("api_key")
        try:
            user = User.objects.get(api_key=api_key)
        except User.DoesNotExist:
            return render(request, "../templates/submit_job.html", {"error": "Invalid API Key"})

        job_id = uuid.uuid4()
        job_type = request.POST.get("job_type", "script")
        schedule_time = request.POST.get("schedule_time")
        priority = int(request.POST.get("priority", 0))
        task_data = request.POST.get("data", "default")

        data_location = save_to_wasabi(task_data, job_id)

        job = Job.objects.create(
            job_id=job_id,
            user=user,
            job_type=job_type,
            schedule_time=schedule_time,
            priority=priority,
            data_location=data_location,
        )

        return redirect("job_result", job_id=job.job_id)

    return render(request, "../templates/submit_job.html")
def job_result(request, job_id):
    api_key = request.GET.get("api_key")
    if not api_key:
        return JsonResponse({"error": "Missing API key"}, status=401)

    try:
        user = User.objects.get(api_key=api_key)
        job = Job.objects.get(job_id=job_id, user=user)
    except (User.DoesNotExist, Job.DoesNotExist):
        return JsonResponse({"error": "Unauthorized or not found"}, status=403)

    result = None
    if job.status in ("COMPLETED", "ERROR") and job.result_location:
        result = get_from_wasabi(job.result_location)

    return render(request, "../templates/job_result.html", {
        "job_id": job_id,
        "status": job.status,
        "result": result,
    })


# def signup_view(request):
#     if request.method == 'POST':
#         name = request.POST.get('name')
#         email = request.POST.get('email')
#         password = request.POST.get('password')
#         # You can save user logic here later
#         print(name, email, password)
#     return render(request, '../templates/signup.html')

import uuid
from django.shortcuts import render, redirect
from scheduler.models import User
from django.contrib import messages

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

from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login
from django.contrib import messages

from django.contrib.auth import authenticate, login
from django.shortcuts import render, redirect
from django.contrib import messages

# def login_view(request):
#     if request.method == 'POST':
#         email = request.POST.get('email')
#         password = request.POST.get('password')

#         user = authenticate(request, username=email, password=password)

#         if user is not None:
#             login(request, user)
#             return redirect('job_list')  # Redirect to the page showing submitted jobs
#         else:
#             messages.error(request, 'Invalid email or password.')

#     return render(request, '../templates/login.html')
from django.shortcuts import render, redirect
from scheduler.models import User
from django.contrib import messages

def login_view(request):
    if request.method == 'POST':
        email = request.POST.get('email')
        password = request.POST.get('password')  # Assuming you are storing passwords directly (not recommended in production)

        try:
            user = User.objects.get(username=email, api_key=password)
            request.session['user_id'] = user.user_id
            request.session['username'] = user.username
            return redirect('job_list')
        except User.DoesNotExist:
            messages.error(request, 'Invalid credentials.')

    return render(request, '../templates/login.html')
