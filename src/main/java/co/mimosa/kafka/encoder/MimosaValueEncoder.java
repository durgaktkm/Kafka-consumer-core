package co.mimosa.kafka.encoder;

import co.mimosa.kafka.valueobjects.GateWayData;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.LoggerFactory;

/**
 * Created by ramdurga on 1/5/15.
 */
public class MimosaValueEncoder implements Encoder<GateWayData> {
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(MimosaValueEncoder.class);
  public MimosaValueEncoder(VerifiableProperties verifiableProperties) {

  }

  @Override
  public byte[] toBytes(GateWayData data) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(data).getBytes();
    } catch (Exception e) {
      logger.debug("Exception while converting object to json "+e.getMessage());
      throw new RuntimeException(e);
    }

  }
}
