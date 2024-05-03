package io.github.sullis.testcontainers.playground.s3;

import io.github.sullis.testcontainers.playground.CloudRuntime;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractS3Test {
  private static final List<SdkAsyncHttpClient.Builder<?>> ASYNC_HTTP_CLIENT_BUILDER_LIST = List.of(
      NettyNioAsyncHttpClient.builder(),
      AwsCrtAsyncHttpClient.builder());

  private static final List<SdkHttpClient.Builder<?>> SYNC_HTTP_CLIENT_BUILDER_LIST = List.of(
      ApacheHttpClient.builder(),
      AwsCrtHttpClient.builder());

  private final Logger logger = LoggerFactory.getLogger(getClass());

  protected abstract List<CloudRuntime> s3Runtimes();

  public List<S3AsyncClientInfo> s3AsyncClients() {
    List<S3AsyncClientInfo> result = new ArrayList<>();
    for (CloudRuntime s3Runtime : s3Runtimes()) {
      ASYNC_HTTP_CLIENT_BUILDER_LIST.forEach(httpClientBuilder -> {
        var httpClient = httpClientBuilder.build();
        S3AsyncClient s3Client =
            (S3AsyncClient) s3Runtime.configure(S3AsyncClient.builder().httpClient(httpClient)).build();
        result.add(new S3AsyncClientInfo(httpClient.clientName(), s3Runtime, s3Client));
      });

      // S3 crtBuilder
      result.add(new S3AsyncClientInfo("crtBuilder", s3Runtime, s3Runtime.configure(S3AsyncClient.crtBuilder()).build()));
    }

    return result;
  }

  public List<S3ClientInfo> s3Clients() {
    List<S3ClientInfo> result = new ArrayList<>();
    for (CloudRuntime s3Runtime: s3Runtimes()) {
      SYNC_HTTP_CLIENT_BUILDER_LIST.forEach(httpClientBuilder -> {
        var httpClient = httpClientBuilder.build();
        S3Client s3Client =
            (S3Client) s3Runtime.configure(S3Client.builder().httpClient(httpClient)).build();
        result.add(new S3ClientInfo(httpClient.clientName(), s3Runtime, s3Client));
      });
    }

    return result;
  }

  @ParameterizedTest
  @MethodSource("s3AsyncClients")
  public void validateS3AsyncClient(S3AsyncClientInfo s3ClientInfo) throws Exception {
    final S3AsyncClient s3Client = s3ClientInfo.client;
    final String bucket = "bucket-" + UUID.randomUUID();
    CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucket).build();
    CreateBucketResponse createBucketResponse = s3Client.createBucket(createBucketRequest).get();
    assertSuccess(createBucketResponse);

    final String key = "key-" + UUID.randomUUID();
    CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build();
    CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest).get();
    assertSuccess(createMultipartUploadResponse);

    final String uploadId = createMultipartUploadResponse.uploadId();

    List<CompletedPart> completedParts = new ArrayList<>();
    final String partText = StringUtils.repeat("a", 6_000_000);

    for (int part = 1; part <= 3; part++) {
      AsyncRequestBody requestBody = AsyncRequestBody.fromString(partText);
      UploadPartRequest uploadPartRequest =
          UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(part).build();
      UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest, requestBody).get();
      assertSuccess(uploadPartResponse);
      logger.info("uploaded part " + part + " using s3 client: " + s3ClientInfo);
      completedParts.add(CompletedPart.builder().partNumber(part).eTag(uploadPartResponse.eTag()).build());
    }

    CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts).build();

    CompleteMultipartUploadRequest
        completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).multipartUpload(completedMultipartUpload).build();
    CompleteMultipartUploadResponse
        completeMultipartUploadResponse = s3Client.completeMultipartUpload(completeMultipartUploadRequest).get();
    assertSuccess(completeMultipartUploadResponse);

    Path localPath = Path.of(Files.temporaryFolderPath() + "/" + UUID.randomUUID().toString());
    File localFile = localPath.toFile();

    GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
    GetObjectResponse getObjectResponse = s3Client.getObject(getObjectRequest, localFile.toPath()).get();
    assertSuccess(getObjectResponse);
    assertThat(localFile).exists();
    assertThat(localFile).hasSize(18000000L);
  }

  @ParameterizedTest
  @MethodSource("s3Clients")
  public void validateS3Client(S3ClientInfo s3ClientInfo) throws Exception {
    final S3Client s3Client = s3ClientInfo.client;
    final String bucket = "bucket-" + UUID.randomUUID();
    CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucket).build();
    CreateBucketResponse createBucketResponse = s3Client.createBucket(createBucketRequest);
    assertSuccess(createBucketResponse);

    final String key = "key-" + UUID.randomUUID();
    CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build();
    CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest);
    assertSuccess(createMultipartUploadResponse);

    final String uploadId = createMultipartUploadResponse.uploadId();

    List<CompletedPart> completedParts = new ArrayList<>();
    final String partText = StringUtils.repeat("a", 6_000_000);

    for (int part = 1; part <= 3; part++) {
      RequestBody requestBody = RequestBody.fromString(partText);
      UploadPartRequest uploadPartRequest =
          UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(part).build();
      UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest, requestBody);
      assertSuccess(uploadPartResponse);
      logger.info("uploaded part " + part + " using s3 client: " + s3ClientInfo);
      completedParts.add(CompletedPart.builder().partNumber(part).eTag(uploadPartResponse.eTag()).build());
    }

    CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts).build();

    CompleteMultipartUploadRequest
        completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).multipartUpload(completedMultipartUpload).build();
    CompleteMultipartUploadResponse
        completeMultipartUploadResponse = s3Client.completeMultipartUpload(completeMultipartUploadRequest);
    assertSuccess(completeMultipartUploadResponse);

    Path localPath = Path.of(Files.temporaryFolderPath() + "/" + UUID.randomUUID().toString());
    File localFile = localPath.toFile();

    GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
    GetObjectResponse getObjectResponse = s3Client.getObject(getObjectRequest, localFile.toPath());
    assertSuccess(getObjectResponse);
    assertThat(localFile).exists();
    assertThat(localFile).hasSize(18000000L);
  }

  private static void assertSuccess(final SdkResponse sdkResponse) {
    assertThat(sdkResponse.sdkHttpResponse().isSuccessful()).isTrue();
  }

  public record S3AsyncClientInfo(String httpClientDescription, CloudRuntime cloudRuntime, S3AsyncClient client) {
    @Override
    public String toString() {
      return cloudRuntime.getClass().getSimpleName() + ":" + httpClientDescription + ":" + this.client.getClass().getSimpleName();
    }
  }

  public record S3ClientInfo(String httpClientDescription, CloudRuntime cloudRuntime, S3Client client) {
    @Override
    public String toString() {
      return cloudRuntime.getClass().getSimpleName() + ":" + httpClientDescription + ":" + this.client.getClass().getSimpleName();
    }
  }
}