package com.shzhangji.mapred_sandbox.kafka;

public class OffsetRange {

  private long fromOffset;
  private long untilOffset;

  public OffsetRange(long fromOffset, long untilOffset) {
    this.fromOffset = fromOffset;
    this.untilOffset = untilOffset;
  }

  public long getFromOffset() {
    return fromOffset;
  }

  public long getUntilOffset() {
    return untilOffset;
  }

}
