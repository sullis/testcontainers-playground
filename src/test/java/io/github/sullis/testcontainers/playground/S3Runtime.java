package io.github.sullis.testcontainers.playground;

import java.net.URI;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;


public interface S3Runtime {
  S3CrtAsyncClientBuilder configure(S3CrtAsyncClientBuilder builder);
  AwsClientBuilder<?, ?> configure(AwsClientBuilder<?, ?> builder);

  class LocalstackS3 implements S3Runtime {
    private final LocalStackContainer container;
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final Region awsRegion;

    public LocalstackS3(LocalStackContainer container) {
      if (!container.isRunning()) {
        throw new IllegalStateException("container is not running");
      }
      this.container = container;
      this.awsCredentialsProvider = StaticCredentialsProvider.create(
          AwsBasicCredentials.create(container.getAccessKey(), container.getSecretKey())
      );
      this.awsRegion = Region.of(container.getRegion());
    }

    @Override
    public S3CrtAsyncClientBuilder configure(S3CrtAsyncClientBuilder builder) {
      return builder.endpointOverride(container.getEndpoint())
          .credentialsProvider(awsCredentialsProvider)
          .region(awsRegion);
    }

    @Override
    public AwsClientBuilder<?, ?> configure(AwsClientBuilder<?, ?> builder) {
      return builder.endpointOverride(container.getEndpoint())
          .credentialsProvider(awsCredentialsProvider)
          .region(awsRegion);
    }
  }

  class MinioS3 implements S3Runtime {
    private final MinIOContainer container;
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final Region awsRegion;
    private final URI endpoint;

    public MinioS3(MinIOContainer container) {
      if (!container.isRunning()) {
        throw new IllegalStateException("container is not running");
      }
      this.container = container;
      this.awsCredentialsProvider = StaticCredentialsProvider.create(
          AwsBasicCredentials.create("dummy", "dummy")
      );
      this.awsRegion = Region.US_EAST_1;
      this.endpoint = URI.create("http://" + container.getHost() + ":" + container.getFirstMappedPort());
      String s3Url = container.getS3URL();
      System.out.println("foo: " +container.getS3URL());
    }

    @Override
    public S3CrtAsyncClientBuilder configure(S3CrtAsyncClientBuilder builder) {
      return builder.endpointOverride(endpoint)
          .credentialsProvider(awsCredentialsProvider)
          .region(awsRegion);
    }

    @Override
    public AwsClientBuilder<?, ?> configure(AwsClientBuilder<?, ?> builder) {
      return builder.endpointOverride(endpoint)
          .credentialsProvider(awsCredentialsProvider)
          .region(awsRegion);
    }
  }
}