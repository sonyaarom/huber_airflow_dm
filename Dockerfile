# Use an official Airflow image as the base image
FROM apache/airflow:2.10.2

# Switch to root to install system dependencies
USER root

# Install system dependencies if any are needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Copy the requirements file into the image
COPY --chown=airflow:root requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Set the PYTHONPATH to ensure installed packages are found
ENV PYTHONPATH="${PYTHONPATH}:/home/airflow/.local/lib/python3.7/site-packages"

# If you have any additional configuration or setup steps, add them here

# The CMD instruction should be taken care of by the base image, 
# so we don't need to specify it unless you want to override it