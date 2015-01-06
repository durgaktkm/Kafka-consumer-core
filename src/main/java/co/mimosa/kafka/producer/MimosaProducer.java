package co.mimosa.kafka.producer;

import co.mimosa.kafka.valueobjects.GateWayData;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by ramdurga on 12/5/14.
 */
public class MimosaProducer {
  private static Producer<String, GateWayData> producer;
  private final Properties props = new Properties();
  public MimosaProducer(String brokerList)
  {
    props.put("metadata.broker.list", brokerList);
    props.put("serializer.class", "co.mimosa.kafka.encoder.MimosaValueEncoder");
    props.put("partitioner.class", "co.mimosa.kafka.producer.MimosaSerialNumberPartitoner");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    producer = new Producer<String, GateWayData>(new
        ProducerConfig(props));

  }

  public void sendDataToKafka(KeyedMessage<String, GateWayData> data){
     producer.send(data);
  }

  public void sendDataToKafka(String topic,GateWayData gatewayData){
    KeyedMessage<String, GateWayData> data = new KeyedMessage<String,
        GateWayData>(topic, gatewayData);
    producer.send(data);
  }
  public void sendDataToKafka(String topic,String key,GateWayData gatewayData){
    KeyedMessage<String, GateWayData> data = new KeyedMessage<String,
        GateWayData>(topic,key, gatewayData);
    producer.send(data);
  }

  public void close(){
    producer.close();
  }


}
