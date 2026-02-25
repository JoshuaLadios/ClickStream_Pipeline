FROM python:3.11.2

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws
    
# Install Java
RUN apt-get update && \
    apt-get install -y \
        default-jre-headless \
        build-essential \
        python3-dev \
        libffi-dev \
        libssl-dev \
        && rm -rf /var/lib/apt/lists/*

# Set Java path (optional, but good)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /clickstream

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# COPY ../script script

# CMD ["python", "script/clickstream.py"]
CMD ["sleep", "infinity"]