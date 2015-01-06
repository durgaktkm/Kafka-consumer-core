package co.mimosa.kafka.producer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class MimosaSerialNumberPartitoner implements Partitioner {
  public MimosaSerialNumberPartitoner(VerifiableProperties props){
    super();
  }
  @Override
  public int partition(Object key, int numPartitions) {
    int partition = 0;
    //int iKey =(Integer) key;
    if (key != null && ((String)key).trim()!="") {
      partition = Math.abs(key.hashCode()) % numPartitions;
    }
    return partition;
  }


}
