FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /app
COPY gradle/ gradle/
COPY gradlew build.gradle settings.gradle ./
RUN ./gradlew dependencies --no-daemon || true
COPY src/ src/
RUN ./gradlew bootJar --no-daemon -x test

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/build/libs/mp-db-1.0.0-SNAPSHOT.jar app.jar
VOLUME /app/data
ENTRYPOINT ["java", "-jar", "app.jar"]
