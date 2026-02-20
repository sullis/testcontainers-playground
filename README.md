# testcontainers-playground

A Java playground for experimenting with [Testcontainers](https://java.testcontainers.org) and AWS services via [LocalStack](https://localstack.cloud).

## What's inside

- **LocalStack** container spun up via Testcontainers for local AWS service emulation
- **AWS SDK v2** async client tests against:
  - DynamoDB (create table, put/get item)
  - S3 (create bucket, put/get object)
  - Kinesis (create stream, put record)
  - CloudWatch (put metric data, list metrics)
- Tests run with both **Netty** and **AWS CRT** async HTTP clients via JUnit 5 parameterized tests

## Requirements

- Java 17+ (tested with Java 21)
- Docker (required by Testcontainers)
- Maven

## Running the tests

```bash
mvn test
```

## Tech stack

| Dependency | Version |
|---|---|
| Testcontainers | 2.0.3 |
| JUnit Jupiter | 6.1.0-M1 |
| AWS SDK v2 | 2.40.5 |
| AssertJ | 4.0.0-M1 |
| Logback | 1.5.32 |

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)

