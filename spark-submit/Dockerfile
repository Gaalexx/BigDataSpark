FROM apache/spark:3.5.6

WORKDIR /opt/spark-app

COPY --chmod=755 entrypoint.sh /opt/spark-submit-entrypoint.sh

ENTRYPOINT ["/opt/spark-submit-entrypoint.sh"]
