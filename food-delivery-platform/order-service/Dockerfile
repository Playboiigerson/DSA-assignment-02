FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/bin/order_service.jar app.jar
COPY Config.docker.toml Config.toml
EXPOSE 9003
CMD ["java", "-jar", "app.jar"]
