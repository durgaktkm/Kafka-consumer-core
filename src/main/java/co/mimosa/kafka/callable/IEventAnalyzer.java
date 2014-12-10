package co.mimosa.kafka.callable;

/**
 * Created by ramdurga on 11/30/14.
 */
public interface IEventAnalyzer {
  public Boolean analyze(String message);
}
