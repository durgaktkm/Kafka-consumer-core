package co.mimosa.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by ramdurga on 12/5/14.
 */
public class MimosaProducer {
  private static Producer<Integer, String> producer;
  private final Properties props = new Properties();
  public MimosaProducer(String brokerList)
  {
    props.put("metadata.broker.list", brokerList);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "co.mimosa.kafka.producer.MimosaSerialNumberPartitoner");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    producer = new Producer<Integer, String>(new
        ProducerConfig(props));

  }

  public void sendDataToKafka(KeyedMessage<Integer, String> data){
     producer.send(data);
  }

  public void sendDataToKafka(String topic,String messageStr){
    KeyedMessage<Integer, String> data = new KeyedMessage<Integer,
        String>(topic, messageStr);
    producer.send(data);
  }
  public void sendDataToKafka(String topic,Integer key,String messageStr){
    KeyedMessage<Integer, String> data = new KeyedMessage<Integer,
        String>(topic,key, messageStr);
    producer.send(data);
  }

  public void close(){
    producer.close();
  }


}
