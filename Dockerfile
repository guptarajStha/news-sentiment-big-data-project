# Base image with Spark + Python
FROM bitnami/spark:latest

# Install system dependencies for Pillow and ping
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    nano \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /opt/news-sentimental

# Copy all project files into container
COPY . /opt/news-sentimental

# Upgrade pip and install Python packages
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
