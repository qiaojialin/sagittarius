package com.sagittarius.write.internals;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * handles the sending of records to the server
 */
public class Sender implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Sender.class);

    private Session session;
    private final RecordAccumulator accumulator;
    private Monitor monitor;
    private AtomicBoolean flag = new AtomicBoolean(true);
    public Sender(Session session, RecordAccumulator accumulator) {
        this.session = session;
        this.accumulator = accumulator;
        this.monitor = new BasicMonitor();
    }

    public Sender(Session session, RecordAccumulator accumulator, Monitor monitor) {
        this.session = session;
        this.accumulator = accumulator;
        this.monitor = monitor;
    }


    @Override
    public void run() {
        while (flag.get()) {
            try {
                run(System.currentTimeMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void run(long nowMs) throws InterruptedException {
        RecordBatch batch = accumulator.getReadyBatch(nowMs);
        if (batch != null) {
            try {
                int records = batch.batchStatement.size();
                session.execute(batch.sendableStatement());
                batch.batchStatement.getStatements();
//                monitor.inform(batch.getRawData());
                logger.debug("Auto-batch thread insert {} records to database", records);
            } catch (Exception e) {
                monitor.inform(batch.getRawData());
                logger.error("Auto-batch thread throws exception while insert records to database", e);
            }
        }
    }

    public void close(){
        flag.set(false);
        monitor.stop();
    }
}
