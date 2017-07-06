package com.sagittarius.bean.result;

import com.sagittarius.bean.common.ValueType;

/**
 * Created by Leah on 2017/6/13.
 */
public class BatchPoint {
    private String host;
    private String metric;
    private long primaryTime;
    private long sencondaryTime;
    private ValueType valueType;
    private Object value;
    private Object latitude;
    private Object longitude;

    public BatchPoint(String host, String metric, long primaryTime, long sencondaryTime, ValueType valueType, Object value){
        this.host = host;
        this.metric = metric;
        this.primaryTime = primaryTime;
        this.sencondaryTime = sencondaryTime;
        this.value = value;
        this.valueType = valueType;
    }

    public BatchPoint(String host, String metric, long primaryTime, long sencondaryTime, ValueType valueType, float latitude, float longitude){
        this.host = host;
        this.metric = metric;
        this.primaryTime = primaryTime;
        this.sencondaryTime = sencondaryTime;
        this.latitude = latitude;
        this.longitude = longitude;
        this.valueType = valueType;
    }

    public String getHost(){return host;}
    public String getMetric(){return metric;}
    public long getPrimaryTime(){return primaryTime;}
    public long getSencondaryTime(){return sencondaryTime;}
    public ValueType getValueType(){return valueType;}
    public Object getValue(){return value;}

}
