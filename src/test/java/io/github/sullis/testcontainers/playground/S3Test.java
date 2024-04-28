package io.github.sullis.testcontainers.playground;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;


public class S3Test {
  private static final List<SdkAsyncHttpClient.Builder<?>> ASYNC_HTTP_CLIENT_BUILDER_LIST = List.of(
      NettyNioAsyncHttpClient.builder(),
      AwsCrtAsyncHttpClient.builder());

  private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.3.0"))
      .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3, LocalStackContainer.Service.KINESIS);

  private static final AwsCredentialsProvider AWS_CREDENTIALS_PROVIDER = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())
  );

  private static final Region AWS_REGION = Region.of(LOCALSTACK.getRegion());

  private static final MinIOContainer MINIO_CONTAINER = new MinIOContainer(DockerImageName.parse("minio/minio:latest"));

  private static final Logger LOGGER = LoggerFactory.getLogger(S3Test.class);

  @BeforeAll
  public static void startContainers() {
    LOCALSTACK.start();
    MINIO_CONTAINER.start();
  }

  @AfterAll
  public static void stopContainers() {
    if (LOCALSTACK != null) {
      LOCALSTACK.stop();
    }
    if (MINIO_CONTAINER != null) {
      MINIO_CONTAINER.stop();
    }
  }

  static Stream<Arguments> awsSdkAsyncHttpClients() {
    return Stream.of(
        arguments("nettyAsyncHttpClient", NettyNioAsyncHttpClient.builder().build()),
        arguments("crtAsyncHttpClient", AwsCrtAsyncHttpClient.builder().build())
    );
  }

  public static List<S3AsyncClientInfo> s3AsyncClients() {
    List<S3AsyncClientInfo> result = new ArrayList<>();
    ASYNC_HTTP_CLIENT_BUILDER_LIST.forEach(httpClientBuilder -> {
      var httpClient = httpClientBuilder.build();
      S3AsyncClient s3Client = (S3AsyncClient) configure(S3AsyncClient.builder().httpClient(httpClient)).build();
      result.add(new S3AsyncClientInfo(httpClient.clientName(), s3Client));
    });

    // S3 crtBuilder
    result.add(new S3AsyncClientInfo("crtBuilder", configure(S3AsyncClient.crtBuilder()).build()));

    return result;
  }

  @ParameterizedTest
  @MethodSource("s3AsyncClients")
  public void s3(S3AsyncClientInfo s3ClientInfo) throws Exception {
    final S3AsyncClient s3Client = s3ClientInfo.client;
    final byte[] payload = "payload123".getBytes(StandardCharsets.UTF_8);
    final String bucket = "bucket-" + UUID.randomUUID();
    final String pathToFile = "/path/" + UUID.randomUUID();
    final String location = "s3://" + bucket + pathToFile;
    CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucket).build();
    CreateBucketResponse createBucketResponse = s3Client.createBucket(createBucketRequest).get();
    assertSuccess(createBucketResponse);

    final String key = "key-" + UUID.randomUUID();
    CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build();
    CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest).get();
    assertSuccess(createMultipartUploadResponse);

    final String uploadId = createMultipartUploadResponse.uploadId();

    List<CompletedPart> completedParts = new ArrayList<>();

    /* TODO
    for (int part = 1; part <= 3; part++) {
      AsyncRequestBody requestBody = AsyncRequestBody.fromString("Hello world");
      UploadPartRequest uploadPartRequest =
          UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(part).build();
      UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest, requestBody).get();
      assertSuccess(uploadPartResponse);
      LOGGER.info("uploaded part " + part);
      completedParts.add(CompletedPart.builder().partNumber(part).build());
    }

    CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts).build();

    CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).multipartUpload(completedMultipartUpload).build();
    CompleteMultipartUploadResponse completeMultipartUploadResponse = s3Client.completeMultipartUpload(completeMultipartUploadRequest).get();
    assertSuccess(completeMultipartUploadResponse);

     */
  }

  private static S3CrtAsyncClientBuilder configure(S3CrtAsyncClientBuilder builder) {
    return builder.endpointOverride(LOCALSTACK.getEndpoint())
        .credentialsProvider(AWS_CREDENTIALS_PROVIDER)
        .region(AWS_REGION);
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