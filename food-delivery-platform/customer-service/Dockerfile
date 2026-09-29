FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/bin/customer_service.jar app.jar
COPY Config.docker.toml Config.toml
EXPOSE 9001
CMD ["java", "-jar", "app.jar"]
