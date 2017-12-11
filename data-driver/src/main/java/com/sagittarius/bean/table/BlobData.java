package com.sagittarius.bean.table;

import com.datastax.driver.mapping.annotations.Column;

import java.nio.ByteBuffer;

public class BlobData extends AbstractData {
    private ByteBuffer value;

    public BlobData(String host, String metric, String timeSlice, long primaryTime, Long secondaryTime, ByteBuffer value) {
        super(host, metric, timeSlice, primaryTime, secondaryTime);
        this.value = value;
    }

    public BlobData() {
    }

    @Column(name = "value")
    public ByteBuffer getValue() {
        return value;
    }

    public void setValue(ByteBuffer value) {
        this.value = value;
    }
}
