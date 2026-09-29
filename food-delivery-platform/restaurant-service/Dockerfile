FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/bin/restaurant_service.jar app.jar
COPY Config.docker.toml Config.toml
EXPOSE 9002
CMD ["java", "-jar", "app.jar"]
