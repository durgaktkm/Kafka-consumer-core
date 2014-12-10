package co.mimosa.kafka.consumer;

import co.mimosa.kafka.callable.IEventAnalyzer;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import kafka.javaapi.consumer.ConsumerConnector;
import org.junit.Before;
import org.junit.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class KafkaMultiThreadedConsumerTest {
  private ConsumerConnector consumer;

  private String topic;
  private ThreadPoolTaskExecutor executorService;
  private boolean started = false;
  private int phase = 1;
  private String numThreads;
  private String zookeeperConnection;
  private String groupId;
  private String zookeeperSessionTimeOutMs;
  private String zookeeperSyncTimeMs;
  private String autoCommitIntervalMs;
  private AmazonS3Client s3;
  private ProfileCredentialsProvider awsCredentials;
  private String s3_bucket;
  private String s3_id;
  private IEventAnalyzer eventAnalyzer;
  KafkaMultiThreadedConsumer kafkaMultiThreadedConsumer;

  @Before
  public void setUp() throws Exception {
    consumer = mock(ConsumerConnector.class);
    topic ="topic";
    executorService = mock(ThreadPoolTaskExecutor.class);
    numThreads = "1";
    zookeeperConnection ="localhost:2181";
    groupId ="group1";
    zookeeperSessionTimeOutMs="123";
    zookeeperSyncTimeMs="234";
    autoCommitIntervalMs="12";
    s3 = mock(AmazonS3Client.class);
    awsCredentials = mock(ProfileCredentialsProvider.class);
    s3_bucket="bucket";
    s3_id="id";
    eventAnalyzer = mock(IEventAnalyzer.class);
    kafkaMultiThreadedConsumer = new KafkaMultiThreadedConsumer(topic,executorService,numThreads,
        zookeeperConnection,groupId,zookeeperSessionTimeOutMs,zookeeperSyncTimeMs,autoCommitIntervalMs,
       eventAnalyzer);
  }

  @Test
  public void testIsAutoStartup() throws Exception {
    assertThat(kafkaMultiThreadedConsumer.isAutoStartup()).isTrue();
  }

  @Test
  public void testStop() throws Exception {
    kafkaMultiThreadedConsumer.start();
    assertThat(kafkaMultiThreadedConsumer.isRunning()).isTrue();
    kafkaMultiThreadedConsumer.stop();
    assertThat(true).isTrue();
  }

  @Test
  public void testStart() throws Exception {
    kafkaMultiThreadedConsumer.start();
    assertThat(kafkaMultiThreadedConsumer.isRunning()).isTrue();
  }

  @Test
  public void testStop1() throws Exception {
    kafkaMultiThreadedConsumer.start();
    assertThat(kafkaMultiThreadedConsumer.isRunning()).isTrue();
    kafkaMultiThreadedConsumer.stop();
    verify(executorService).shutdown();

    assertThat(true).isTrue();
  }

  @Test
  public void testIsRunning() throws Exception {
    kafkaMultiThreadedConsumer.start();
    assertThat(kafkaMultiThreadedConsumer.isRunning()).isTrue();
    kafkaMultiThreadedConsumer.stop();
    assertThat(kafkaMultiThreadedConsumer.isRunning()).isFalse();
  }

  @Test
  public void testGetPhase() throws Exception {
    assertThat(kafkaMultiThreadedConsumer.getPhase()).isEqualTo(1);
  }
}