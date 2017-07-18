package com.sagittarius.example;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.StringFilter;
import com.sagittarius.bean.result.FloatPoint;
import com.sagittarius.bean.result.StringPoint;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.exceptions.UnregisteredHostMetricException;
import com.sagittarius.write.SagittariusWriter;
import com.sagittarius.write.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by hadoop on 17-3-22.
 */
public class BatchWriteBigDataTask2 extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(BatchWriteTask.class);

    private SagittariusWriter writer;
    private int order;
    private int batchSize;
    private List<String> metrics;
    private int throught;
    public HashMap<String, ArrayList<Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float>>> map;

    public BatchWriteBigDataTask2(SagittariusWriter writer, int order, int batchSize, List<String> metrics, HashMap<String, ArrayList<Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float>>> map){
        this.writer = writer;
        this.order = order;
        this.batchSize = batchSize;
        this.map = map;
        this.metrics = metrics;
        this.throught = 0;
    }

    public int getThrought(){
        return throught;
    }

    @Override
    public void run() {

        for(int i = 0; i < 50; i++){
            SagittariusWriter.Data datas = writer.newData();
            int count = 0;
            Set<String> hosts = map.keySet();
            //252
            for(String host : hosts){
                String newHost = host + (1000+50*order+i);
                //50 * 20
                    ArrayList<Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float>> data = map.get(host);
                    //for every line data
                    for(Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float> d : data){
                        long primaryTime = d._1().getPrimaryTime();
                        long secondaryTime = d._1().getSecondaryTime();
                        if(d._1().getValue() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(0), primaryTime, secondaryTime, d._1().getValue());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._2() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(1), primaryTime, secondaryTime, d._2());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._3() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(2), primaryTime, secondaryTime, d._3());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._4() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(3), primaryTime, secondaryTime, d._4());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._5() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(4), primaryTime, secondaryTime, d._5());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._6() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(5), primaryTime, secondaryTime, d._6());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._7() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(6), primaryTime, secondaryTime, d._7());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._8() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(7), primaryTime, secondaryTime, d._8());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }if(d._9() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(8), primaryTime, secondaryTime, d._9());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._10() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(9), primaryTime, secondaryTime, d._10());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._11() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(10), primaryTime, secondaryTime, d._11());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._12() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(11), primaryTime, secondaryTime, d._12());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._13() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(12), primaryTime, secondaryTime, d._13());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._14() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(13), primaryTime, secondaryTime, d._14());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._15() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(14), primaryTime, secondaryTime, d._15());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._16() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(15), primaryTime, secondaryTime, d._16());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._17() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(16), primaryTime, secondaryTime, d._17());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(!d._18().equals("")){
                            try {
                                datas.addDatum(newHost, metrics.get(17), primaryTime, secondaryTime, d._18());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._19() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(18), primaryTime, secondaryTime, d._19());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._20() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(19), primaryTime, secondaryTime, d._20());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._21() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(20), primaryTime, secondaryTime, d._21());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(d._22() != -1){
                            try {
                                datas.addDatum(newHost, metrics.get(21), primaryTime, secondaryTime, d._22());
                            } catch (UnregisteredHostMetricException e) {
                                e.printStackTrace();
                            }
                            count += 1;
                        }
                        if(count > batchSize){
                            throught += count;
                            try {
                                writer.bulkInsert(datas);
                            } catch (Exception e) {
                                e.getMessage();
                            }
                            count = 0;
                            datas = writer.newData();
                        }
                    }
                }
        }
    }
}
