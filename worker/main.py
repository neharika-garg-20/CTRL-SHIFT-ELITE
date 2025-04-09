import os
import json
import time
import smtplib
import requests
import shutil
import zipfile
import psycopg2
from kafka import KafkaConsumer
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv


load_dotenv()

# === Email Config ===
EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", 587))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

# === Kafka Config ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "job-queue")

# === Wasabi/S3 Config ===
WASABI_BUCKET = os.getenv("WASABI_BUCKET")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_ENDPOINT = os.getenv("WASABI_ENDPOINT")

def send_email_notification(subject, body, recipient):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = recipient
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))
    
    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.send_message(msg)
        print(f"📧 Email sent to {recipient}")

def health_check(urls):
    for url in urls:
        try:
            response = requests.get(url, timeout=5)
            print(f"✅ {url} is UP (Status {response.status_code})")
        except requests.exceptions.RequestException:
            print(f"❌ {url} is DOWN")

def perform_backup(source_path, dest_zip_path):
    zipf = zipfile.ZipFile(dest_zip_path, 'w', zipfile.ZIP_DEFLATED)
    for root, _, files in os.walk(source_path):
        for file in files:
            filepath = os.path.join(root, file)
            arcname = os.path.relpath(filepath, start=source_path)
            zipf.write(filepath, arcname)
    zipf.close()
    print(f"🗂️ Backup created at {dest_zip_path}")
    
    # Optionally upload to Wasabi (placeholder function)
    upload_to_wasabi(dest_zip_path)

def upload_to_wasabi(file_path):
    print(f"📤 Uploading {file_path} to Wasabi (mocked)")

def process_job(job):
    job_type = job.get("job_type")
    print(f"🚀 Processing job: {job_type}")

    if job_type == "health_check":
        urls = job.get("data", {}).get("urls", [])
        health_check(urls)

    elif job_type == "email":
        data = job.get("data", {})

        recipient = data.get("recipient")
        subject = data.get("subject", "No Subject")
        body = data.get("body", "")

        if not recipient:
            print("❌ Email job missing recipient.")
        else:
            send_email_notification(subject, body, recipient)

    elif job_type == "backup":
        data = job.get("data", {})
        source_path = data.get("source_path")
        backup_name = data.get("backup_name", "backup.zip")
        if not source_path:
            print("❌ Backup job missing source_path.")
        else:
            perform_backup(source_path, backup_name)

    else:
        print(f"⚠️ Unknown job type: {job_type}")

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='worker-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("🔁 Worker listening for jobs...")

    for message in consumer:
        job = message.value
        process_job(job)
        time.sleep(1)

if __name__ == "__main__":
    main()
