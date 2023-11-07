FROM python:3.8-bullseye

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Jupyter and other python deps
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Add scala kernel via spylon-kernel
RUN python3 -m spylon_kernel install

# Download and install IJava jupyter kernel
RUN curl https://github.com/SpencerPark/IJava/releases/download/v1.3.0/ijava-1.3.0.zip -Lo ijava-1.3.0.zip \
  && unzip ijava-1.3.0.zip \
  && python3 install.py --sys-prefix \
  && rm ijava-1.3.0.zip

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

WORKDIR ${SPARK_HOME}

ENV SPARK_VERSION=3.4.1
ENV SPARK_MAJOR_VERSION=3.4
ENV ICEBERG_VERSION=1.3.0

# Download spark
RUN mkdir -p ${SPARK_HOME} \
 && curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Download iceberg spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# Download nessie spark runtime
RUN curl https://repo.maven.apache.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-${SPARK_MAJOR_VERSION}_2.12/0.65.1/nessie-spark-extensions-${SPARK_MAJOR_VERSION}_2.12-0.65.1.jar -Lo /opt/spark/jars/nessie-spark-extensions-${SPARK_MAJOR_VERSION}_2.12-0.65.1.jar

# Download JDBC driver for Postgres
RUN curl https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -Lo /opt/spark/jars/postgresql-42.6.0.jar

# Download Java AWS SDK
ENV AWSSDK_VERSION=2.20.18
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWSSDK_VERSION}/bundle-${AWSSDK_VERSION}.jar -Lo /opt/spark/jars/bundle-${AWSSDK_VERSION}.jar

# Download URL connection client required for S3FileIO
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWSSDK_VERSION}/url-connection-client-${AWSSDK_VERSION}.jar -Lo /opt/spark/jars/url-connection-client-${AWSSDK_VERSION}.jar

# Add iceberg spark runtime jar to IJava classpath
ENV IJAVA_CLASSPATH=/opt/spark/jars/*

RUN mkdir -p /root/.ipython/profile_default/startup

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

EXPOSE 8888

CMD [ "jupyter-lab", "--ip=0.0.0.0", "--port=8888", "--allow-root"]