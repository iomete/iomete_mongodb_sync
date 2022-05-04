FROM iomete/spark-py:3.1.3.0

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add additional jars to the spark classpath
COPY ./docker/jars /opt/spark/jars

# add source code to the /app directory
COPY setup.py setup.cfg driver.py ./
COPY iomete_mongodb_sync ./iomete_mongodb_sync

RUN pip install .

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
