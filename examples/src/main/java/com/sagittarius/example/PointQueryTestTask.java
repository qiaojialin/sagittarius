package com.sagittarius.example;

import com.sagittarius.read.Reader;

import java.util.List;

/**
 * Created by Leah on 2017/4/24.
 */
public class PointQueryTestTask extends Thread {
    private Reader reader;
    private List<String> hosts;
    private List<String> metrics;
    private long time;
    private int deviceInternal;

    public PointQueryTestTask(Reader reader, List<String> hosts, List<String> metrics, long time, int internal){
        this.reader = reader;
        this.hosts = hosts;
        this.metrics = metrics;
        this.time = time;
        this.deviceInternal = internal;
    }

    @Override
    public void run() {
        int hostStep = hosts.size() / deviceInternal;
        int metricStep = metrics.size() / deviceInternal;
    }
}
