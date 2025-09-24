# ---------- Build ----------
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /app

COPY pom.xml ./
RUN mvn -q -e -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -q -e -DskipTests package

# ---------- Runtime ----------
FROM eclipse-temurin:17-jre
WORKDIR /opt/app

COPY --from=build /app/target/*.jar app.jar

ENV JAVA_OPTS=""

EXPOSE 8088

# Instalamos wait-for-it para asegurarnos que dependencias estén listas
RUN apt-get update && apt-get install -y wait-for-it && rm -rf /var/lib/apt/lists/*

# Esperamos a Kafka y Redis (y opcionalmente MongoDB)
ENTRYPOINT ["sh", "-c", "wait-for-it kafka:29092 --timeout=60 --strict && wait-for-it redis:6379 --timeout=30 --strict && wait-for-it mongodb:27017 --timeout=30 --strict && java $JAVA_OPTS -jar app.jar"]
