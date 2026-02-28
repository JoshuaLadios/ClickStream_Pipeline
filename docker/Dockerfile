FROM python:3.11.2

# Install Java
RUN apt-get update && \
    apt-get install -y \
        default-jre-headless \
        build-essential \
        python3-dev \
        libffi-dev \
        libssl-dev \
        && rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Set Java path (optional, but good)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /clickstream

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Install Hadoop AWS jars to avoid Ivy resolution
RUN curl -L -o /usr/local/lib/python3.11/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L -o /usr/local/lib/python3.11/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# COPY ../script script

# CMD ["python", "script/clickstream.py"]
CMD ["sleep", "infinity"]