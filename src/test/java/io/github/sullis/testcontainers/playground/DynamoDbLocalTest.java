package io.github.sullis.testcontainers.playground;

import java.net.URI;
import java.util.HashMap;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
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

import static io.github.sullis.testcontainers.playground.AwsTestSupport.assertSuccess;
import static io.github.sullis.testcontainers.playground.AwsTestSupport.configure;
import static org.assertj.core.api.Assertions.assertThat;


public class DynamoDbLocalTest {

  private static final int DYNAMODB_PORT = 8000;

  private static final GenericContainer<?> DYNAMODB = new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:3.3.1"))
      .withExposedPorts(DYNAMODB_PORT);

  @BeforeAll
  public static void startDynamoDb() {
    DYNAMODB.start();
  }

  @AfterAll
  public static void stopDynamoDb() {
    if (DYNAMODB != null) {
      DYNAMODB.stop();
    }
  }

  @ParameterizedTest
  @MethodSource("io.github.sullis.testcontainers.playground.AwsTestSupport#awsSdkAsyncHttpClients")
  public void dynamoDb(final String sdkHttpClientName, final SdkAsyncHttpClient sdkHttpClient) throws Throwable {
    final String key = "key-" + UUID.randomUUID();
    final String keyVal = "keyVal-" + UUID.randomUUID();
    final String tableName = "table-" + UUID.randomUUID();

    try (DynamoDbAsyncClient dbClient = createDynamoDbClient(sdkHttpClient)) {
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
      WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest).get();
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
  }

  private DynamoDbAsyncClient createDynamoDbClient(final SdkAsyncHttpClient sdkHttpClient) {
    return configure(DynamoDbAsyncClient.builder().httpClient(sdkHttpClient), endpoint()).build();
  }

  private static URI endpoint() {
    return URI.create("http://" + DYNAMODB.getHost() + ":" + DYNAMODB.getMappedPort(DYNAMODB_PORT));
  }

}
