FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/bin/notification_service.jar app.jar
COPY Config.docker.toml Config.toml
CMD ["java", "-jar", "app.jar"]
