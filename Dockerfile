FROM python:3.10-bullseye as spark-base

ARG SPARK_VERSION=3.3.2

# Install tools required by the OS
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      iputils-ping \
      software-properties-common \
      ssh && \
      apt-get install -y git && \
      apt-get install -y --no-install-recommends libpq-dev && \
      apt-get install -y wget netcat procps libpostgresql-jdbc-java && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Setup the directories for our Spark and Hadoop installations
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
ENV AWS_HOME=${AWS_HOME:-"/opt/aws"}

RUN mkdir -p ${AWS_HOME}
WORKDIR ${AWS_HOME}

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
unzip awscliv2.zip && \
./aws/install

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download and install Spark
RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && ln -s /usr/share/java/postgresql-jdbc4.jar ${SPARK_HOME}/jars/postgresql-jdbc4.jar \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz


FROM spark-base as pyspark

# Install python deps
COPY ./requirements.txt .
RUN pip3 install -r requirements.txt

# Setup Spark related environment variables
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# Copy the default configurations into $SPARK_HOME/conf
COPY ./conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"
COPY ./conf/hive-site.xml "$SPARK_HOME/conf/hive-site.xml"

#Copy jars
COPY ./jars "$SPARK_HOME/jars/"


RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copy appropriate entrypoint script
COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
CMD ["--help"]
