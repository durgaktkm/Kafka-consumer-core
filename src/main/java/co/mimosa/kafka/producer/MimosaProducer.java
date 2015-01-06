package co.mimosa.kafka.producer;

import co.mimosa.kafka.encoder.MimosaSerializerDeserializer;
import co.mimosa.kafka.valueobjects.GateWayData;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by ramdurga on 12/5/14.
 */
public class MimosaProducer {
  private static Producer<String, byte[]> producer;
  private final Properties props = new Properties();
  public MimosaProducer(String brokerList)
  {
    props.put("metadata.broker.list", brokerList);
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("partitioner.class", "co.mimosa.kafka.producer.MimosaSerialNumberPartitoner");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    producer = new Producer<String, byte[]>(new
        ProducerConfig(props));

  }


  public void sendDataToKafka(String topic,String key,GateWayData gatewayData){
    KeyedMessage<String, byte[]> data = new KeyedMessage<String,
        byte[]>(topic,key, MimosaSerializerDeserializer.toBytes(gatewayData));
    producer.send(data);
  }

  public void close(){
    producer.close();
  }


}
