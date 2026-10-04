# testcontainers-playground

A Java playground for experimenting with [Testcontainers](https://java.testcontainers.org) and local AWS service emulators.

## What's inside

Each test class starts one container for the whole class and exercises the AWS SDK v2 **async** clients against it:

| Test class | Container | Service | What the test does |
|---|---|---|---|
| [`DynamoDbLocalTest`](src/test/java/io/github/sullis/testcontainers/playground/DynamoDbLocalTest.java) | `amazon/dynamodb-local:3.3.1` | DynamoDB | create table, wait until it exists, put item, get item (consistent read), delete table |
| [`S3MockTest`](src/test/java/io/github/sullis/testcontainers/playground/S3MockTest.java) | `adobe/s3mock:5.2.3` | S3 | create bucket, put object, get object as a blocking input stream |
| [`LocalstackTest`](src/test/java/io/github/sullis/testcontainers/playground/LocalstackTest.java) | `localstack/localstack:4.4.0` | Kinesis | create stream, wait until it exists, put record |
| [`LocalstackTest`](src/test/java/io/github/sullis/testcontainers/playground/LocalstackTest.java) | `localstack/localstack:4.4.0` | CloudWatch | put metric data, list metrics — currently `@Disabled` |

LocalStack is pinned to `4.4.0`, a release from before March 2026 that runs without an auth token. Releases since `2026.3.0` require a LocalStack account and token.

Every test is a JUnit 5 parameterized test that runs twice, once per async HTTP client:

- `NettyNioAsyncHttpClient`
- `AwsCrtAsyncHttpClient`

## Requirements

- Java 17+ (CI and `.sdkmanrc` use Java 21)
- Docker (required by Testcontainers)
- Maven

## Running the tests

```bash
mvn test
```

Surefire is configured with `forkCount=8` and `reuseForks=true`, so tests run in parallel across forks.

## Tech stack

Versions are managed in [`pom.xml`](pom.xml), which is the source of truth:

| Dependency | Version |
|---|---|
| Testcontainers | 2.0.5 |
| JUnit | 6.1.3 |
| AWS SDK v2 | 2.54.17 |
| AssertJ | 4.0.0-M1 |
| Apache Commons Lang | 3.20.0 |
| SLF4J | 2.0.19 |
| Logback | 1.6.3 |

Dependency updates are automated with Dependabot, and CI builds on every push and pull request to `main`.

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)

## Other projects

- https://github.com/redis-field-engineering/testcontainers-redis
- https://github.com/sullis/redis-playground
- https://github.com/sullis/valkey-playground

