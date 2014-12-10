package co.mimosa.kafka.callable;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Created by ramdurga on 11/30/14.
 */
public class KafkaConsumerCallable implements Callable{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaConsumerCallable.class);
  private ConsumerConnector consumer;
  private IEventAnalyzer analyzer;
  private KafkaStream stream;
  private int threadNumber;

  public KafkaConsumerCallable(ConsumerConnector consumer,IEventAnalyzer analyzer,KafkaStream stream, int threadNumber){
    this.consumer = consumer;
    this.analyzer = analyzer;
    this.stream = stream;
    this.threadNumber = threadNumber;
  }



  @Override
  public Object call() throws Exception {
    for (MessageAndMetadata<byte[], byte[]> aStream : (Iterable<MessageAndMetadata<byte[], byte[]>>) stream) {
      logger.debug("Message from thread " + threadNumber + ": ");
      String message = new String(aStream.message());
      logger.debug("Message from thread " + threadNumber + ": " + message);
      Boolean analyze = analyzer.analyze(message);
      if(analyze)consumer.commitOffsets();
    }
    return true;
  }
}
