FROM docker.io/maven AS miniaccumulo-build-env
WORKDIR /app
COPY pom.xml ./
COPY . ./
RUN mvn clean package -Dmaven.test.skip=true

# Build runtime image.
FROM openjdk:8-jre-alpine
COPY --from=miniaccumulo-build-env /app/target/accumulo-mini-test-1.0-SNAPSHOT.jar /app/accumulo-mini.jar
CMD ["java", "-cp", "/app/accumulo-mini.jar", "dmk.accumulo.mini.MiniAccumulo"]
