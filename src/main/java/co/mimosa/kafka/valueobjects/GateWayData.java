package co.mimosa.kafka.valueobjects;

import java.io.Serializable;

/**
 * Created by ramdurga on 1/5/15.
 */
public class GateWayData implements Serializable{
  long timeStamp;
  boolean isFileData;
  String fileName;
  boolean isCompressed;
  String contents;

  public GateWayData(long timeStamp, boolean isFileData, String fileName, boolean isCompressed, String contents) {
    this.timeStamp = timeStamp;
    this.isFileData = isFileData;
    this.fileName = fileName;
    this.isCompressed = isCompressed;
    this.contents = contents;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public boolean isFileData() {
    return isFileData;
  }

  public String getFileName() {
    return fileName;
  }

  public boolean isCompressed() {
    return isCompressed;
  }

  public String getContents() {
    return contents;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof GateWayData))
      return false;

    GateWayData that = (GateWayData) o;

    if (isCompressed != that.isCompressed)
      return false;
    if (isFileData != that.isFileData)
      return false;
    if (timeStamp != that.timeStamp)
      return false;
    if (contents != null ? !contents.equals(that.contents) : that.contents != null)
      return false;
    if (fileName != null ? !fileName.equals(that.fileName) : that.fileName != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (timeStamp ^ (timeStamp >>> 32));
    result = 31 * result + (isFileData ? 1 : 0);
    result = 31 * result + (fileName != null ? fileName.hashCode() : 0);
    result = 31 * result + (isCompressed ? 1 : 0);
    result = 31 * result + (contents != null ? contents.hashCode() : 0);
    return result;
  }

  @Override public String toString() {
    return "GateWayData{" +
        "timeStamp=" + timeStamp +
        ", isFileData=" + isFileData +
        ", fileName='" + fileName + '\'' +
        ", isCompressed=" + isCompressed +
        ", contents='" + contents + '\'' +
        '}';
  }
}
