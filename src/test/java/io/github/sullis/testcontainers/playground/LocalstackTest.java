package io.github.sullis.testcontainers.playground;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.client.builder.SdkAsyncClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkSyncClientBuilder;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.s3.S3Client;
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
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;


public class LocalstackTest {
  private static final SdkHttpClient.Builder<?> AWS_SDK_HTTP_CLIENT_BUILDER = ApacheHttpClient.builder();
  private static final SdkAsyncHttpClient.Builder<?> AWS_SDK_ASYNC_HTTP_CLIENT_BUILDER = NettyNioAsyncHttpClient.builder();

  private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.3.0"))
      .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3, LocalStackContainer.Service.KINESIS);

  private static final AwsCredentialsProvider AWS_CREDENTIALS_PROVIDER = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())
  );

  private static final Region AWS_REGION = Region.of(LOCALSTACK.getRegion());

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


  static Stream<Arguments> awsSdkHttpClients() {
    return Stream.of(
        arguments("nettyHttpClient", NettyNioAsyncHttpClient.builder().build()),
        arguments("crtHttpClient", AwsCrtAsyncHttpClient.builder().build())
    );
  }

  @ParameterizedTest
  @MethodSource("awsSdkHttpClients")
  public void dynamodDb(final String sdkHttpClientName, final SdkAsyncHttpClient sdkHttpClient) throws Throwable {
    final String key = "key-" + UUID.randomUUID().toString();
    final String keyVal = "keyVal-" + UUID.randomUUID().toString();
    final String tableName = "table-" + UUID.randomUUID().toString();

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

  @Test
  public void s3() throws Exception {
    final byte[] payload = "payload123".getBytes(StandardCharsets.UTF_8);
    final String bucket = "bucket-" + UUID.randomUUID();
    final String pathToFile = "/path/" + UUID.randomUUID();
    final String location = "s3://" + bucket + pathToFile;
    try (S3Client s3Client = createS3Client()) {
      CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucket).build();
      CreateBucketResponse createBucketResponse = s3Client.createBucket(createBucketRequest);
      assertSuccess(createBucketResponse);
    }
  }

  @Test
  public void kinesis() throws Exception {
    final String streamName = UUID.randomUUID().toString();
    final String payload = "{}";
    try (KinesisClient kinesisClient = createKinesisClient()) {
      CreateStreamResponse createStreamResponse = kinesisClient.createStream(builder -> {
        builder.streamName(streamName).shardCount(10);
      });
      assertSuccess(createStreamResponse);
      kinesisClient.waiter().waitUntilStreamExists(builder -> {
        builder.streamName(streamName).build();
      });
      PutRecordResponse putRecordResponse = kinesisClient.putRecord(builder -> {
        builder.streamName(streamName)
            .data(SdkBytes.fromString(payload, StandardCharsets.UTF_8))
            .partitionKey("foobar");
      });
      assertSuccess(putRecordResponse);
    }
  }

  private DynamoDbAsyncClient createDynamoDbClient(final SdkAsyncHttpClient sdkHttpClient) {
    return  DynamoDbAsyncClient.builder()
        .httpClient(sdkHttpClient)
        .endpointOverride(LOCALSTACK.getEndpoint())
        .credentialsProvider(AWS_CREDENTIALS_PROVIDER)
        .region(AWS_REGION)
        .build();
  }

  private S3Client createS3Client() {
    return (S3Client) configure(S3Client.builder()).build();
  }

  private KinesisClient createKinesisClient() {
    return (KinesisClient) configure(KinesisClient.builder()).build();
  }

  private AwsClientBuilder<?, ?> configure(AwsClientBuilder<?, ?> builder) {
    if (builder instanceof SdkSyncClientBuilder) {
      ((SdkSyncClientBuilder<?, ?>) builder).httpClient(AWS_SDK_HTTP_CLIENT_BUILDER.build());
    } else if (builder instanceof SdkAsyncClientBuilder<?,?>) {
      ((SdkAsyncClientBuilder<?, ?>) builder).httpClient(AWS_SDK_ASYNC_HTTP_CLIENT_BUILDER.build());
    }else {
      throw new IllegalStateException("unexpected AwsClientBuilder");
    }
    return builder.endpointOverride(LOCALSTACK.getEndpoint())
        .credentialsProvider(AWS_CREDENTIALS_PROVIDER)
        .region(AWS_REGION);
  }

  private static void assertSuccess(final SdkResponse sdkResponse) {
    assertThat(sdkResponse.sdkHttpResponse().isSuccessful()).isTrue();
  }
}
