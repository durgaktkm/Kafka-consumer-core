package co.mimosa.kafka.encoder;

import co.mimosa.kafka.valueobjects.GateWayData;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by ramdurga on 1/6/15.
 */
public class MimosaSerializerDeserializer {
  private static org.slf4j.Logger logger = LoggerFactory.getLogger(MimosaSerializerDeserializer.class);
  public static byte[] toBytes(GateWayData data){
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(data);
      byte[] bytes = bos.toByteArray();
      return bytes;
    } catch (IOException e) {
      logger.debug("Message  while getting bytes from GateWayData object"+e);
      e.printStackTrace();
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } catch (IOException ex) {
        // ignore close exception
      }
      try {
        bos.close();
      } catch (IOException ex) {
        // ignore close exception
      }
    }
    return null;
  }
  public static GateWayData fromBytes(byte[] data){
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    ObjectInput in = null;
    try {
      in = new ObjectInputStream(bis);
      return (GateWayData) in.readObject();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      logger.debug("Message  while getting GateWayData object from bytes"+e);
      e.printStackTrace();
    } finally {
      try {
        bis.close();
      } catch (IOException ex) {
        // ignore close exception
      }
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException ex) {
        // ignore close exception
      }
    }
    return null;
  }
}
