package io.github.sullis.testcontainers.playground;

import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
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

  private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.3.0"))
      .withServices(LocalStackContainer.Service.DYNAMODB);

  private static final AwsCredentialsProvider CREDENTIALS_PROVIDER = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())
  );

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


  static Stream<Arguments> params() {
    return Stream.of(
        arguments("nettyHttpClient", NettyNioAsyncHttpClient.builder().build()),
        arguments("crtHttpClient", AwsCrtAsyncHttpClient.builder().build())
    );
  }

  @ParameterizedTest
  @MethodSource("params")
  public void dynamodDb(final String sdkHttpClientName, final SdkAsyncHttpClient sdkHttpClient) throws Throwable {
    final String key = "key-" + UUID.randomUUID().toString();
    final String keyVal = "keyVal-" + UUID.randomUUID().toString();
    final String tableName = "table-" + UUID.randomUUID().toString();

    DynamoDbAsyncClient dbClient = DynamoDbAsyncClient.builder()
        .httpClient(sdkHttpClient)
        .endpointOverride(LOCALSTACK.getEndpoint())
        .credentialsProvider(CREDENTIALS_PROVIDER)
        .region(Region.of(LOCALSTACK.getRegion()))
        .build();
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
    assertThat(describeTableResponse.sdkHttpResponse().isSuccessful()).isTrue();
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
    assertThat(putItemResponse.sdkHttpResponse().isSuccessful()).isTrue();

    HashMap<String, AttributeValue> getItemValues = new HashMap<>();
    getItemValues.put(key, AttributeValue.builder().s(keyVal).build());
    GetItemRequest getItemRequest = GetItemRequest.builder()
        .tableName(tableName)
        .key(getItemValues)
        .consistentRead(true)
        .build();
    GetItemResponse getItemResponse = dbClient.getItem(getItemRequest).get();
    assertThat(getItemResponse.sdkHttpResponse().isSuccessful()).isTrue();
    assertThat(getItemResponse.item().keySet()).containsExactlyInAnyOrder("country", "city", key);

    DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
        .tableName(tableName)
        .build();
    DeleteTableResponse deleteTableResponse = dbClient.deleteTable(deleteTableRequest).get();
    assertThat(deleteTableResponse.sdkHttpResponse().isSuccessful()).isTrue();

  }
}
