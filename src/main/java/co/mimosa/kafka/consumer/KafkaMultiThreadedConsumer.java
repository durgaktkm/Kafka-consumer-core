package co.mimosa.kafka.consumer;

import co.mimosa.kafka.callable.IEventAnalyzer;
import co.mimosa.kafka.callable.KafkaConsumerCallable;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by ramdurga on 11/30/14.
 */
public class KafkaMultiThreadedConsumer implements SmartLifecycle {
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaMultiThreadedConsumer.class);
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

  private IEventAnalyzer eventAnalyzer;

  public KafkaMultiThreadedConsumer(String topic, ThreadPoolTaskExecutor executorService,
      String numThreads, String zookeeperConnection, String groupId, String zookeeperSessionTimeOutMs,
      String zookeeperSyncTimeMs, String autoCommitIntervalMs, IEventAnalyzer eventAnalyzer) {
    this.topic = topic;
    this.executorService = executorService;
    this.numThreads = numThreads;
    this.zookeeperConnection = zookeeperConnection;
    this.groupId = groupId;
    this.zookeeperSessionTimeOutMs = zookeeperSessionTimeOutMs;
    this.zookeeperSyncTimeMs = zookeeperSyncTimeMs;
    this.autoCommitIntervalMs = autoCommitIntervalMs;

    this.eventAnalyzer = eventAnalyzer;
  }

  public KafkaMultiThreadedConsumer() {

  }

  @Override public void start() {
    consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
    Map<String, Integer> topicCount = new HashMap<>();
    topicCount.put(topic, Integer.parseInt(numThreads));

    Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
    List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);

    int threadNumber = 0;
    for (final KafkaStream stream : streams) {
      executorService
          .submit(new KafkaConsumerCallable(consumer,eventAnalyzer, stream, threadNumber));
      threadNumber++;
    }
    started = true;
  }

  @Override
  public boolean isRunning() {
    return started;
  }

  @Override
  public void stop() {
    if (started) {
      consumer.shutdown();
      executorService.shutdown();
    }
    started = false;
  }

  @Override
  public int getPhase() {
    return phase;
  }

  @Override
  public boolean isAutoStartup() {
    return true;
  }

  @Override
  public void stop(Runnable runnable) {
    stop();
    runnable.run();
  }

  private ConsumerConfig createConsumerConfig() {
    Properties props = new Properties();
    props.put("zookeeper.connect", zookeeperConnection);
    props.put("group.id", groupId);
    props.put("zookeeper.session.timeout.ms", zookeeperSessionTimeOutMs);
    props.put("zookeeper.sync.time.ms", zookeeperSyncTimeMs);
    props.put("auto.commit.enable","false");
    //props.put("auto.commit.interval.ms", autoCommitIntervalMs);
    return new ConsumerConfig(props);
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public void setPhase(int phase) {
    this.phase = phase;
  }

  public void setNumThreads(String numThreads) {
    this.numThreads = numThreads;
  }

  public void setZookeeperConnection(String zookeeperConnection) {
    this.zookeeperConnection = zookeeperConnection;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public void setZookeeperSessionTimeOutMs(String zookeeperSessionTimeOutMs) {
    this.zookeeperSessionTimeOutMs = zookeeperSessionTimeOutMs;
  }

  public void setZookeeperSyncTimeMs(String zookeeperSyncTimeMs) {
    this.zookeeperSyncTimeMs = zookeeperSyncTimeMs;
  }

  public void setAutoCommitIntervalMs(String autoCommitIntervalMs) {
    this.autoCommitIntervalMs = autoCommitIntervalMs;
  }

  public void setExecutorService(ThreadPoolTaskExecutor executorService) {
    this.executorService = executorService;
  }

  public void setEventAnalyzer(IEventAnalyzer eventAnalyzer) {
    this.eventAnalyzer = eventAnalyzer;
  }
}
