from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import boto3
from django.conf import settings
import uuid
from .models import User, Job

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

@csrf_exempt
def submit_job(request):
    if request.method == "POST":
        api_key = request.headers.get("X-API-Key")
        if not api_key:
            return JsonResponse({"error": "No API key provided"}, status=401)
        
        try:
            user = User.objects.get(api_key=api_key)
        except User.DoesNotExist:
            return JsonResponse({"error": "Invalid API key"}, status=401)
        
        data = request.POST
        job_id = uuid.uuid4()
        schedule_time = data.get("schedule_time")
        job_type = data.get("job_type", "script")
        priority = int(data.get("priority", 0))
        task_data = data.get("data", "default_task")
        
        data_location = save_to_wasabi(task_data, job_id)
        
        job = Job(
            job_id=job_id,
            user=user,
            job_type=job_type,
            schedule_time=schedule_time,
            priority=priority,
            data_location=data_location
        )
        job.save()
        
        return JsonResponse({"job_id": str(job_id)}, status=202)
    return JsonResponse({"error": "Method not allowed"}, status=405)

def get_job_results(request, job_id):
    if request.method == "GET":
        api_key = request.headers.get("X-API-Key")
        if not api_key:
            return JsonResponse({"error": "No API key provided"}, status=401)
        
        try:
            user = User.objects.get(api_key=api_key)
        except User.DoesNotExist:
            return JsonResponse({"error": "Invalid API key"}, status=401)
        
        try:
            job = Job.objects.get(job_id=job_id, user=user)
        except Job.DoesNotExist:
            return JsonResponse({"error": "Job not found or not yours"}, status=403)
        
        if job.status in ("COMPLETED", "ERROR") and job.result_location:
            result = get_from_wasabi(job.result_location)
            return JsonResponse({"status": job.status, "result": result}, status=200)
        return JsonResponse({"status": job.status}, status=200)
    return JsonResponse({"error": "Method not allowed"}, status=405)