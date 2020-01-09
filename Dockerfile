FROM openjdk:8-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD target/transitdata-omm-cancellation-source.jar /usr/app/transitdata-omm-cancellation-source.jar
ENTRYPOINT ["java", "-jar", "/usr/app/transitdata-omm-cancellation-source.jar"]
