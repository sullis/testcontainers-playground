package io.github.sullis.testcontainers.playground;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
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

import static io.github.sullis.testcontainers.playground.AwsTestSupport.assertSuccess;
import static org.assertj.core.api.Assertions.assertThat;


public class LocalstackTest {

  private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.4.0"))
      .withServices(
          "cloudwatch",
          "kinesis");

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

  @ParameterizedTest
  @MethodSource("io.github.sullis.testcontainers.playground.AwsTestSupport#awsSdkAsyncHttpClients")
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
  @MethodSource("io.github.sullis.testcontainers.playground.AwsTestSupport#awsSdkAsyncHttpClients")
  @Disabled
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

  private KinesisAsyncClient createKinesisClient(final SdkAsyncHttpClient sdkHttpClient) {
    return (KinesisAsyncClient) configure(KinesisAsyncClient.builder().httpClient(sdkHttpClient)).build();
  }

  private CloudWatchAsyncClient createCloudWatchClient(final SdkAsyncHttpClient sdkHttpClient) {
    return (CloudWatchAsyncClient) configure(CloudWatchAsyncClient.builder().httpClient(sdkHttpClient)).build();
  }

  private static AwsClientBuilder<?, ?> configure(AwsClientBuilder<?, ?> builder) {
    return builder.endpointOverride(LOCALSTACK.getEndpoint())
          .credentialsProvider(AWS_CREDENTIALS_PROVIDER)
          .region(AWS_REGION);
  }

}
