package io.github.sullis.testcontainers.playground;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.waiters.WaiterResponse;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;


public class LocalstackTest {
  private static final List<SdkAsyncHttpClient.Builder<?>> ASYNC_HTTP_CLIENT_BUILDER_LIST = List.of(
      NettyNioAsyncHttpClient.builder(),
      AwsCrtAsyncHttpClient.builder());

  private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.3.0"))
      .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3, LocalStackContainer.Service.KINESIS);

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
      PutRecordResponse putRecordResponse = kinesisClient.putRecord(builder -> {
        builder.streamName(streamName)
            .data(SdkBytes.fromString(payload, StandardCharsets.UTF_8))
            .partitionKey("foobar");
      }).get();
      assertSuccess(putRecordResponse);
    }
  }

  private DynamoDbAsyncClient createDynamoDbClient(final SdkAsyncHttpClient sdkHttpClient) {
    return (DynamoDbAsyncClient) configure(DynamoDbAsyncClient.builder().httpClient(sdkHttpClient)).build();
  }

  private KinesisAsyncClient createKinesisClient(final SdkAsyncHttpClient sdkHttpClient) {
    return (KinesisAsyncClient) configure(KinesisAsyncClient.builder().httpClient(sdkHttpClient)).build();
  }

  private static AwsClientBuilder<?, ?> configure(AwsClientBuilder<?, ?> builder) {
    return builder.endpointOverride(LOCALSTACK.getEndpoint())
          .credentialsProvider(AWS_CREDENTIALS_PROVIDER)
          .region(AWS_REGION);
  }

  private static void assertSuccess(final SdkResponse sdkResponse) {
    assertThat(sdkResponse.sdkHttpResponse().isSuccessful()).isTrue();
  }

  public record S3AsyncClientInfo(String description, S3AsyncClient client) {
    @Override
    public String toString() {
      return this.description + ":" + this.client.getClass().getSimpleName();
    }
  }
}