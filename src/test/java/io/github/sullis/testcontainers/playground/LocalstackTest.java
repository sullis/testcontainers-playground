package io.github.sullis.testcontainers.playground;

import java.util.HashMap;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
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
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import static org.assertj.core.api.Assertions.assertThat;


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

  @Test
  public void dynamodDb() {
    final String key = "key-" + UUID.randomUUID().toString();
    final String keyVal = "keyVal-" + UUID.randomUUID().toString();
    final String tableName = "table-" + UUID.randomUUID().toString();

    DynamoDbClient dbClient = DynamoDbClient.builder()
        .endpointOverride(LOCALSTACK.getEndpoint())
        .credentialsProvider(CREDENTIALS_PROVIDER)
        .region(Region.of(LOCALSTACK.getRegion()))
        .build();
    DynamoDbWaiter dbWaiter = dbClient.waiter();
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
    CreateTableResponse response = dbClient.createTable(request);
    assertThat(response.tableDescription().tableName()).isEqualTo(tableName);
    DescribeTableRequest tableRequest = DescribeTableRequest.builder()
        .tableName(tableName)
        .build();
    WaiterResponse<DescribeTableResponse> waiterResponse =  dbWaiter.waitUntilTableExists(tableRequest);
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
    PutItemResponse putItemResponse = dbClient.putItem(putItemRequest);
    assertThat(putItemResponse.sdkHttpResponse().isSuccessful()).isTrue();

    HashMap<String, AttributeValue> getItemValues = new HashMap<>();
    getItemValues.put(key, AttributeValue.builder().s(keyVal).build());
    GetItemRequest getItemRequest = GetItemRequest.builder()
        .tableName(tableName)
        .key(getItemValues)
        .consistentRead(true)
        .build();
    GetItemResponse getItemResponse = dbClient.getItem(getItemRequest);
    assertThat(getItemResponse.sdkHttpResponse().isSuccessful()).isTrue();
    assertThat(getItemResponse.item().keySet()).containsExactly("country", "city", key);

    DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
        .tableName(tableName)
        .build();
    DeleteTableResponse deleteTableResponse = dbClient.deleteTable(deleteTableRequest);
    assertThat(deleteTableResponse.sdkHttpResponse().isSuccessful()).isTrue();

  }
}
