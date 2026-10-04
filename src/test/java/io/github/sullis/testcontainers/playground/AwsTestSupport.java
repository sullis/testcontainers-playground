package io.github.sullis.testcontainers.playground;

import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

final class AwsTestSupport {

  private static final StaticCredentialsProvider DUMMY_CREDENTIALS_PROVIDER = StaticCredentialsProvider.create(
      AwsBasicCredentials.create("test", "test")
  );

  private AwsTestSupport() {
  }

  static Stream<Arguments> awsSdkAsyncHttpClients() {
    return Stream.of(
        arguments("nettyAsyncHttpClient", NettyNioAsyncHttpClient.builder().build()),
        arguments("crtAsyncHttpClient", AwsCrtAsyncHttpClient.builder().build())
    );
  }

  static <B extends AwsClientBuilder<?, ?>> B configure(final B builder, final URI endpoint) {
    builder.endpointOverride(endpoint)
        .credentialsProvider(DUMMY_CREDENTIALS_PROVIDER)
        .region(Region.US_EAST_1);
    return builder;
  }

  static void assertSuccess(final SdkResponse sdkResponse) {
    assertThat(sdkResponse.sdkHttpResponse().isSuccessful()).isTrue();
  }

}
