package com.sagittarius.write.internals;

import com.sagittarius.bean.result.BatchPoint;

import java.util.ArrayList;

/**
 * Created by Leah on 2017/6/13.
 */
public interface Monitor {
    public void inform(ArrayList<BatchPoint> rawData);
    public void stop();
}
