package io.github.sullis.testcontainers.playground;

import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import static io.github.sullis.testcontainers.playground.AwsTestSupport.assertSuccess;
import static io.github.sullis.testcontainers.playground.AwsTestSupport.configure;
import static org.assertj.core.api.Assertions.assertThat;


public class S3MockTest {

  private static final int S3MOCK_HTTP_PORT = 9090;

  private static final GenericContainer<?> S3MOCK = new GenericContainer<>(DockerImageName.parse("adobe/s3mock:5.2.3"))
      .withExposedPorts(S3MOCK_HTTP_PORT);

  @BeforeAll
  public static void startS3Mock() {
    S3MOCK.start();
  }

  @AfterAll
  public static void stopS3Mock() {
    if (S3MOCK != null) {
      S3MOCK.stop();
    }
  }

  @ParameterizedTest
  @MethodSource("io.github.sullis.testcontainers.playground.AwsTestSupport#awsSdkAsyncHttpClients")
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

  private S3AsyncClient createS3Client(final SdkAsyncHttpClient sdkHttpClient) {
    return configure(S3AsyncClient.builder().httpClient(sdkHttpClient), endpoint())
        .forcePathStyle(true)
        .build();
  }

  private static URI endpoint() {
    return URI.create("http://" + S3MOCK.getHost() + ":" + S3MOCK.getMappedPort(S3MOCK_HTTP_PORT));
  }

}
