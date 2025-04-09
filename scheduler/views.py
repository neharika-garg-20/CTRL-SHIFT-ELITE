from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from scheduler.models import Job, User
from scheduler.serializers import JobSerializer
import boto3
import uuid
from django.conf import settings

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
