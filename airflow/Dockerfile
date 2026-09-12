FROM apache/airflow:2.9.0-python3.11

USER root

# --- System packages ---
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
        ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# --- Resolve JAVA_HOME robustly ---
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java-home
ENV JAVA_HOME=/opt/java-home

# --- Spark client (for spark-submit) ---
ARG SPARK_VERSION=3.5.1
ARG SPARK_HADOOP=3
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP}.tgz" \
      | tar -xz -C /opt && \
    ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP}" /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# --- Hadoop CLI (for hdfs dfs ...) ---
ARG HADOOP_VERSION=3.3.6
RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
      | tar -xz -C /opt && \
    ln -s "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/conf
ENV PATH="${HADOOP_HOME}/bin:${PATH}"

# --- Python deps (needs to run as airflow for pip cache ownership) ---
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.7.1