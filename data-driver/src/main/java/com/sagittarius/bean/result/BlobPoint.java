package com.sagittarius.bean.result;

import java.nio.ByteBuffer;

public class BlobPoint extends AbstractPoint {
    private ByteBuffer value;

    public BlobPoint(String metric, long primaryTime, long secondaryTime, ByteBuffer value) {
        super(metric, primaryTime, secondaryTime);
        this.value = value;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public void setValue(ByteBuffer value) {
        this.value = value;
    }
}
