package co.mimosa.kafka.callable;

import com.amazonaws.services.s3.AmazonS3;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class KafkaConsumerCallableTest {
  KafkaConsumerCallable callable;
  IEventAnalyzer eventAnalyzer;
  AmazonS3 s3;
  String s3Bucket;
  String s3Id;
  KafkaStream stream;
  int threadNum;
  ConsumerConnector consumer;
  @Before
  public void setUp() throws Exception {
    eventAnalyzer = new IEventAnalyzer() {
       public Boolean analyze(String str) {
          return true;
      }
    };
    s3 = mock(AmazonS3.class);
    s3Bucket = "bucket";
    s3Id = "id";
    stream = mock(KafkaStream.class);
    threadNum =1;
    consumer = mock(ConsumerConnector.class);

  }

  @Test
  public void testCreateKafkaConsumer() throws Exception {
    callable = new KafkaConsumerCallable(consumer,eventAnalyzer,null,threadNum);
    Assertions.assertThat(callable).isNotNull();
  }
  @Test
  public void testCreateKafkaConsumerCall() throws Exception {
    KafkaStream kafkaStream = mock(KafkaStream.class);

    callable = new KafkaConsumerCallable(consumer,eventAnalyzer,kafkaStream,threadNum);
    //when(kafkaStream.)
    Assertions.assertThat(callable).isNotNull();
  }

}