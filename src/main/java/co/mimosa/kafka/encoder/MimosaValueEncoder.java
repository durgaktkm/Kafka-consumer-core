package co.mimosa.kafka.encoder;

import co.mimosa.kafka.valueobjects.GateWayData;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ramdurga on 1/5/15.
 */
public class MimosaValueEncoder implements Encoder<GateWayData> ,Decoder<GateWayData>{
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(MimosaValueEncoder.class);
  private ObjectMapper objectMapper = new ObjectMapper();
  public MimosaValueEncoder(VerifiableProperties verifiableProperties) {

  }

  @Override
  public byte[] toBytes(GateWayData data) {

    try {
      return objectMapper.writeValueAsString(data).getBytes();
    } catch (Exception e) {
      logger.debug("Exception while converting object to json "+e.getMessage());
      throw new RuntimeException(e);
    }

  }

  @Override public GateWayData fromBytes(byte[] bytes) {
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
