FROM apache/spark:3.4.2

ENV SPARK_VERSION=3.4.2
ENV SPARK_MAJOR_VERSION=3.4
ENV ICEBERG_VERSION=1.3.0
ENV AWSSDK_VERSION=2.20.18

# Download iceberg spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# Download nessie spark runtime
RUN curl https://repo.maven.apache.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-${SPARK_MAJOR_VERSION}_2.12/0.74.0/nessie-spark-extensions-${SPARK_MAJOR_VERSION}_2.12-0.74.0.jar -Lo /opt/spark/jars/nessie-spark-extensions-${SPARK_MAJOR_VERSION}_2.12-0.74.0.jar

# Download JDBC driver for Postgres
RUN curl https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -Lo /opt/spark/jars/postgresql-42.6.0.jar

# Download Java AWS SDK
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWSSDK_VERSION}/bundle-${AWSSDK_VERSION}.jar -Lo /opt/spark/jars/bundle-${AWSSDK_VERSION}.jar

# Download URL connection client required for S3FileIO
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWSSDK_VERSION}/url-connection-client-${AWSSDK_VERSION}.jar -Lo /opt/spark/jars/url-connection-client-${AWSSDK_VERSION}.jar

USER root
RUN pip install psycopg2-binary

USER ${SPARK_UID}

RUN mkdir -p /opt/spark/logs/spark-events
COPY src/spark /spark