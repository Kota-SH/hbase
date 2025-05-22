package org.apache.hadoop.hbase.master;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public class MetaRefreshException extends Throwable {
  public MetaRefreshException(String message) {
    super(message);
  }

  public MetaRefreshException(String message, Throwable cause) {
    super(message, cause);
  }
}
