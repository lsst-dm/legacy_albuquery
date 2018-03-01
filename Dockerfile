FROM openjdk:8-jdk-alpine

ARG JAR_FILE

WORKDIR /opt/lsst/services
COPY target/${JAR_FILE} /opt/lsst/services/app.jar
EXPOSE 8080
CMD ["java", "-jar", "app.jar", "server", "albuquery.yml"]
