import boto3
from botocore.client import Config

# Wasabi credentials
WASABI_ACCESS_KEY = 'AUPWZZXX4J4UVI6QPP62'
WASABI_SECRET_KEY = 'Me1eSZtwzllRLNtdQBW5wgF3UfzzYrvz1ZpiRwq6'
WASABI_BUCKET_NAME = 'schedular'
WASABI_ENDPOINT = 'https://s3.ap-southeast-1.wasabisys.com'

# Configure Wasabi client
wasabi = boto3.client(
    's3',
    endpoint_url=WASABI_ENDPOINT,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# Create the file content
content = "email:neha@example.com:Hello, this is a test notification"

# Upload to Wasabi
try:
    wasabi.put_object(
        Bucket=WASABI_BUCKET_NAME,
        Key='jobs/test/notification.txt',
        Body=content.encode('utf-8')
    )
    print("Uploaded test file to Wasabi successfully")
except Exception as e:
    print(f"Failed to upload to Wasabi: {str(e)}")