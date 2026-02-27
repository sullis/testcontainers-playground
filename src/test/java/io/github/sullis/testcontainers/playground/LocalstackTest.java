package io.github.sullis.testcontainers.playground;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsResponse;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;


public class LocalstackTest {

  private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.4.0"))
      .withServices(
          "cloudwatch",
          "dynamodb",
          "s3",
          "kinesis");

  private static final AwsCredentialsProvider AWS_CREDENTIALS_PROVIDER = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())
  );

  private static final Region AWS_REGION = Region.of(LOCALSTACK.getRegion());

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalstackTest.class);

  @BeforeAll
  public static void startLocalstack() {
    LOCALSTACK.start();
  }

  @AfterAll
  public static void stopLocalstack() {
    if (LOCALSTACK != null) {
      LOCALSTACK.stop();
    }
  }

  static Stream<Arguments> awsSdkAsyncHttpClients() {
    return Stream.of(
        arguments("nettyAsyncHttpClient", NettyNioAsyncHttpClient.builder().build()),
        arguments("crtAsyncHttpClient", AwsCrtAsyncHttpClient.builder().build())
    );
  }

  @ParameterizedTest
  @MethodSource("awsSdkAsyncHttpClients")
  public void dynamodDb(final String sdkHttpClientName, final SdkAsyncHttpClient sdkHttpClient) throws Throwable {
    final String key = "key-" + UUID.randomUUID();
    final String keyVal = "keyVal-" + UUID.randomUUID();
    final String tableName = "table-" + UUID.randomUUID();

    final DynamoDbAsyncClient dbClient = createDynamoDbClient(sdkHttpClient);

    DynamoDbAsyncWaiter dbWaiter = dbClient.waiter();
    CreateTableRequest request = CreateTableRequest.builder()
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName(key)
            .attributeType(ScalarAttributeType.S)
            .build())
        .keySchema(KeySchemaElement.builder()
            .attributeName(key)
            .keyType(KeyType.HASH)
            .build())
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(10L)
            .writeCapacityUnits(10L)
            .build())
        .tableName(tableName)
        .build();
    CreateTableResponse response = dbClient.createTable(request).get();
    assertThat(response.tableDescription().tableName()).isEqualTo(tableName);
    DescribeTableRequest tableRequest = DescribeTableRequest.builder()
        .tableName(tableName)
        .build();
    WaiterResponse<DescribeTableResponse> waiterResponse =  dbWaiter.waitUntilTableExists(tableRequest).get();
    DescribeTableResponse describeTableResponse = waiterResponse.matched().response().get();
    assertThat(describeTableResponse).isNotNull();
    assertSuccess(describeTableResponse);
    assertThat(describeTableResponse.responseMetadata().requestId()).isNotNull();
    assertThat(describeTableResponse.table().tableName()).isEqualTo(tableName);

    HashMap<String, AttributeValue> putItemValues = new HashMap<>();
    putItemValues.put(key, AttributeValue.builder().s(keyVal).build());
    putItemValues.put("city", AttributeValue.builder().s("Seattle").build());
    putItemValues.put("country", AttributeValue.builder().s("USA").build());

    PutItemRequest putItemRequest = PutItemRequest.builder()
        .tableName(tableName)
        .item(putItemValues)
        .build();
    PutItemResponse putItemResponse = dbClient.putItem(putItemRequest).get();
    assertSuccess(putItemResponse);

    HashMap<String, AttributeValue> getItemValues = new HashMap<>();
    getItemValues.put(key, AttributeValue.builder().s(keyVal).build());
    GetItemRequest getItemRequest = GetItemRequest.builder()
        .tableName(tableName)
        .key(getItemValues)
        .consistentRead(true)
        .build();
    GetItemResponse getItemResponse = dbClient.getItem(getItemRequest).get();
    assertSuccess(getItemResponse);
    assertThat(getItemResponse.item().keySet()).containsExactlyInAnyOrder("country", "city", key);

    DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
        .tableName(tableName)
        .build();
    DeleteTableResponse deleteTableResponse = dbClient.deleteTable(deleteTableRequest).get();
    assertSuccess(deleteTableResponse);
  }

  @ParameterizedTest
  @MethodSource("awsSdkAsyncHttpClients")
  public void kinesis(final String sdkHttpClientName, final SdkAsyncHttpClient sdkHttpClient) throws Exception {
    final String streamName = UUID.randomUUID().toString();
    final String payload = "{}";
    try (KinesisAsyncClient kinesisClient = createKinesisClient(sdkHttpClient)) {
      CreateStreamResponse createStreamResponse = kinesisClient.createStream(builder -> {
        builder.streamName(streamName).shardCount(10);
      }).get();
      assertSuccess(createStreamResponse);
      kinesisClient.waiter().waitUntilStreamExists(builder -> {
        builder.streamName(streamName).build();
      }).get();
      String partitionKey = "partition-key-" + ThreadLocalRandom.current().nextInt(0, 10);
      PutRecordResponse putRecordResponse = kinesisClient.putRecord(builder -> {
        builder.streamName(streamName)
            .data(SdkBytes.fromString(payload, StandardCharsets.UTF_8))
            .partitionKey(partitionKey);
      }).get();
      assertSuccess(putRecordResponse);
    }
  }

  @ParameterizedTest
  @MethodSource("awsSdkAsyncHttpClients")
  public void s3(final String sdkHttpClientName, final SdkAsyncHttpClient sdkHttpClient) throws Throwable {
    final String bucketName = "test-bucket-" + UUID.randomUUID().toString();
    final String key = "test-key-" + UUID.randomUUID().toString();
    final String payload = "test-payload-" + UUID.randomUUID().toString();
    try (S3AsyncClient s3Client = createS3Client(sdkHttpClient)) {
      CreateBucketResponse createBucketResponse = s3Client.createBucket(request -> request.bucket(bucketName)).get();
      assertSuccess(createBucketResponse);
      PutObjectResponse putObjectResponse = s3Client.putObject(request -> request.bucket(bucketName).key(key),
          AsyncRequestBody.fromString(payload)).get();
      assertSuccess(putObjectResponse);
      try (ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(request -> request.bucket(bucketName).key(key), AsyncResponseTransformer.toBlockingInputStream()).get()) {
        assertThat(responseInputStream).hasContent(payload);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("awsSdkAsyncHttpClients")
  public void cloudwatch(final String sdkHttpClientName, final SdkAsyncHttpClient sdkHttpClient) throws Throwable {
    final String metricName = "test-metric-name";
    try (CloudWatchAsyncClient cwClient = createCloudWatchClient(sdkHttpClient)) {
        final Double count = 5.0;
        PutMetricDataResponse response = cwClient.putMetricData(
            PutMetricDataRequest.builder()
                .namespace("TestNamespace")
                .metricData(MetricDatum.builder()
                    .metricName(metricName)
                    .unit(StandardUnit.COUNT)
                    .value(count)
                    .build())
                .build()).get();
        assertSuccess(response);
        Awaitility.await()
            .pollInterval(Duration.ofMillis(100))
            .until(() -> {
              ListMetricsResponse listResponse = cwClient.listMetrics().get();
              assertSuccess(listResponse);
              /* todo
              assertThat(listResponse.metrics()).hasSize(1);
              Metric metric = listResponse.metrics().get(0);
              assertThat(metric.metricName()).isEqualTo(metricName);
              */
              return true;
            });
    }
  }

  private DynamoDbAsyncClient createDynamoDbClient(final SdkAsyncHttpClient sdkHttpClient) {
    return (DynamoDbAsyncClient) configure(DynamoDbAsyncClient.builder().httpClient(sdkHttpClient)).build();
  }

  private KinesisAsyncClient createKinesisClient(final SdkAsyncHttpClient sdkHttpClient) {
    return (KinesisAsyncClient) configure(KinesisAsyncClient.builder().httpClient(sdkHttpClient)).build();
  }

  private S3AsyncClient createS3Client(final SdkAsyncHttpClient sdkHttpClient) {
    return (S3AsyncClient) configure(S3AsyncClient.builder().httpClient(sdkHttpClient)).build();
  }

  private CloudWatchAsyncClient createCloudWatchClient(final SdkAsyncHttpClient sdkHttpClient) {
    return (CloudWatchAsyncClient) configure(CloudWatchAsyncClient.builder().httpClient(sdkHttpClient)).build();
  }

  private static AwsClientBuilder<?, ?> configure(AwsClientBuilder<?, ?> builder) {
    return builder.endpointOverride(LOCALSTACK.getEndpoint())
          .credentialsProvider(AWS_CREDENTIALS_PROVIDER)
          .region(AWS_REGION);
  }

  private static void assertSuccess(final SdkResponse sdkResponse) {
    assertThat(sdkResponse.sdkHttpResponse().isSuccessful()).isTrue();
  }

}
