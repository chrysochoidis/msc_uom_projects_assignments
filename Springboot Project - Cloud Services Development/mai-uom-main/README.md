# Car Sales platform

This is a Spring-Boot application handling car sales and reservations for test-drives.

### How to run

You can start the project through an IDE (eg Eclipse), or you can use the maven wrapper with the following command:

`./mvnw clean spring-boot:run`

Then, simply visit [http://localhost:8080](http://localhost:8080) with a browser.

This project uses the H2 in-memory database, so there is no need to setup an external database.

Visit [http://localhost:8080/openapi](http://localhost:8080/openapi) for the OpenAPI specification of the project.

### Initial accounts

For testing convenience we provide the following already initialized accounts:

| Role       | Afm       | Password |
|------------|-----------|----------|
| Client     | 123456789 | 123      |
| Dealership | 123456781 | 123      |

See [InitializationConfig](src/main/java/com/example/ecar/config/InitializationConfig.java) for the full initial data.