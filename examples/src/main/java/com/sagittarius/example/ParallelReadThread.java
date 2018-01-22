package com.sagittarius.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;

public class ParallelReadThread extends Thread {
    private Session session;
    private int id;
    private String query;
    private boolean finished;
    private long consumeTime;

    public ParallelReadThread(Session session, int id, String query) {
        this.session = session;
        this.id = id;
        this.query = query;
    }

    public long getConsumeTime() {
        return consumeTime;
    }

    public boolean isFinished() {
        return finished;
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        ResultSet result = session.execute(query);
        List<Row> rows = result.all();
        long endTime = System.currentTimeMillis();
        consumeTime = endTime-startTime;
        System.out.println(id + "," + consumeTime + "," + rows.size());
        finished = true;
    }
}
