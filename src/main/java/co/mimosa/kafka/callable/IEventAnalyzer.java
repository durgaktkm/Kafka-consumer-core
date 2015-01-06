package co.mimosa.kafka.callable;

import co.mimosa.kafka.valueobjects.GateWayData;

/**
 * Created by ramdurga on 11/30/14.
 */
public interface IEventAnalyzer {
  public Boolean analyze(String Key,GateWayData data);
}
