FROM apache/spark:3.5.6

WORKDIR /opt/spark-app

COPY entrypoint.sh /opt/spark-app/entrypoint.sh
RUN chmod +x /opt/spark-app/entrypoint.sh

ENTRYPOINT ["/opt/spark-app/entrypoint.sh"]