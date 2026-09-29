FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/bin/payment_service.jar app.jar
COPY Config.docker.toml Config.toml
CMD ["java", "-jar", "app.jar"]
