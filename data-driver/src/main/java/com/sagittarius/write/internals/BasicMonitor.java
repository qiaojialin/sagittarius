package com.sagittarius.write.internals;

import com.sagittarius.bean.result.BatchPoint;
import com.sagittarius.util.TimeUtil;

import java.util.ArrayList;

/**
 * Created by Leah on 2017/6/13.
 */
public class BasicMonitor implements Monitor{
    @Override
    public void inform(ArrayList<BatchPoint> rawData) {
        System.out.println(TimeUtil.date2String(System.currentTimeMillis()));
        for(BatchPoint p : rawData){
            System.out.println(p.getHost() + "," + p.getMetric() + "," + p.getPrimaryTime() + "," + p.getSencondaryTime() + "," + p.getValue());
        }
    }

    @Override
    public void stop() {

    }
}
