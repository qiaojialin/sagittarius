package com.sagittarius.write.internals;

import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.common.TimePartition;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * This class acts as a queue that accumulates records to be sent to the server.
 */
public class RecordAccumulator {
    private final int batchSize;
    private final int lingerMs;
    private final LinkedBlockingDeque<RecordBatch> batches;
    private RecordBatch recordBatch;
    private boolean isfull = true;

    private MappingManager mappingManager;

    public RecordAccumulator(int batchSize, int lingerMs, MappingManager mappingManager) {
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.batches = new LinkedBlockingDeque<>();
        this.mappingManager = mappingManager;
        this.recordBatch = null;
    }

    public RecordBatch getReadyBatch(long nowMs) throws InterruptedException {

//        synchronized (batches) {
//            RecordBatch batch = batches.peekFirst();
//            if (batch != null) {
//                long waitedTimeMs = nowMs - batch.createdMs;
//                boolean full = batches.size() > 1 || batch.getRecordCount() >= batchSize;
//                boolean expired = waitedTimeMs >= lingerMs;
//                if (full || expired) {
//                    System.out.println("2");
//                    return batches.pollFirst();
//                }
//            }
//            return null;
//        }

//                long waitedTimeMs = nowMs - recordBatch.createdMs;
//                boolean full = batches.size() > 1 || recordBatch.getRecordCount() >= batchSize;
//                boolean expired = waitedTimeMs >= lingerMs;
//                RecordBatch result = recordBatch;
//                recordBatch = null;
//                if (full || expired) {
//                    isfull = true;
//                    System.out.println(2);
//                    return result;
//                }
//                else {
//                    //if deque is full then we can't put the recordBatch back, what we can only do it to return result
//                    //althought it isn't full
//                    isfull = result.batchStatement.size() > batchSize/2;
//                    return result;
//                }

        if(!isfull){
            Thread.sleep(lingerMs/10);
        }
        recordBatch = batches.pollFirst(lingerMs, TimeUnit.MILLISECONDS);
//        System.out.println(1);
        synchronized (batches) {
            if (recordBatch != null) {
                RecordBatch result = recordBatch;
                recordBatch = null;
                isfull = result.batchStatement.size() > batchSize/2;
                return result;
            }
            return null;
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) {
//        synchronized (batches) {
//            RecordBatch batch = batches.peekLast();
//            if (batch == null || batch.getRecordCount() >= batchSize) {
//                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
//                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
//                batches.addLast(batch);
//            } else {
//                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
//            }
//        }

//                RecordBatch batch = batches.peekLast();
//                if(batch == null){
//                    recordBatch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
//                }
//                else if(batch.getRecordCount() < batchSize){
//                    batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
//                }
//                else{
//                    batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
//                    batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
//                    batches.addLast(batch);
//                }

        synchronized (batches) {
            if(recordBatch == null || recordBatch.getRecordCount() >= batchSize){//利用短路避免npe
                RecordBatch batch = batches.peekLast();
                if (batch == null || batch.getRecordCount() >= batchSize) {
                    batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                    batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                    batches.addLast(batch);
                } else {
                    batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                }
            }
            else {
                recordBatch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            }
        }
    }

    public void append(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) {
        synchronized (batches) {
            RecordBatch batch = batches.peekLast();
            if (batch == null || batch.getRecordCount() >= batchSize) {
                batch = new RecordBatch(System.currentTimeMillis(), mappingManager);
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, latitude, longitude);
                batches.addLast(batch);
            } else {
                batch.append(host, metric, primaryTime, secondaryTime, timePartition, latitude, longitude);
            }
        }
    }
}
