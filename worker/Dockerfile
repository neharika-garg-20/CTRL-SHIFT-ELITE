# Use Python base image
FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy project files into the container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables (optional here, can be used with .env instead)
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Expose port if needed (optional)
# EXPOSE 8000

# Command to run the worker
CMD ["python", "main.py"]
