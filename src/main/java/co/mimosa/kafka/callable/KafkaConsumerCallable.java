package co.mimosa.kafka.callable;

import co.mimosa.kafka.encoder.MimosaSerializerDeserializer;
import co.mimosa.kafka.valueobjects.GateWayData;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
      GateWayData gateWayData = MimosaSerializerDeserializer.fromBytes(aStream.message());
      String key = new String(aStream.key());
      logger.debug("Message from thread " + threadNumber + ": " + gateWayData);
      Boolean analyze = analyzer.analyze(key,gateWayData);
      if(analyze)consumer.commitOffsets();
    }
    return true;
  }

  public GateWayData fromBytes(byte[] bytes) {
    ObjectMapper objectMapper = new ObjectMapper();
    try{
      return objectMapper.readValue(bytes,GateWayData.class);
    } catch (JsonMappingException e) {
      logger.debug("Exception while converting object to json "+e.getMessage());
      e.printStackTrace();
    } catch (JsonParseException e) {
      logger.debug("Exception while converting object to json "+e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      logger.debug("Exception while converting object to json "+e.getMessage());
      e.printStackTrace();
    }
    return null;
  }
}
