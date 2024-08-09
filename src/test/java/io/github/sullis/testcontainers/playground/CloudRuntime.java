package io.github.sullis.testcontainers.playground;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.net.URI;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;


public interface CloudRuntime {
  S3CrtAsyncClientBuilder configure(S3CrtAsyncClientBuilder builder);
  AwsClientBuilder<?, ?> configure(AwsClientBuilder<?, ?> builder);

  class Localstack implements CloudRuntime {
    private final LocalStackContainer container;
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final Region awsRegion;

    public Localstack(LocalStackContainer container) {
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

    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }
  }

  class Aws implements CloudRuntime {

    public Aws() { }

    @Override
    public S3CrtAsyncClientBuilder configure(S3CrtAsyncClientBuilder builder) {
      return builder;
    }

    @Override
    public AwsClientBuilder<?, ?> configure(AwsClientBuilder<?, ?> builder) {
      return builder;
    }
  }
}