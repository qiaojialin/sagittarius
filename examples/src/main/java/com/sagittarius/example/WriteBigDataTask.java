package com.sagittarius.example;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.StringFilter;
import com.sagittarius.bean.result.FloatPoint;
import com.sagittarius.bean.result.StringPoint;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.write.SagittariusWriter;
import com.sagittarius.write.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple22;

import java.util.*;

/**
 * Created by hadoop on 17-3-22.
 */
public class WriteBigDataTask extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(BatchWriteTask.class);

    private SagittariusWriter writer;
    private int order;
    private int batchSize;
    private List<String> metrics;
    private int throught;
    public HashMap<String, ArrayList<Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float>>> map;

    public WriteBigDataTask(SagittariusWriter writer, int order, int batchSize, List<String> metrics, HashMap<String, ArrayList<Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float>>> map){
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

        long primaryTime, secondaryTime;
        long day;
        long lastDay = -1;

        for(int i = 0; i < 50; i++){
            SagittariusWriter.Data datas = writer.newData();
            int count = 0;
            Set<String> hosts = map.keySet();
            //252
            for(String host : hosts){
                String newHost = host + (1000+50*order+i);
                //50 * 20
                for (int team = 100; team < 200; team++){
                    ArrayList<Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float>> data = map.get(host);
                    //for every line data
                    for(Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float> d : data){
                        primaryTime = d._1().getPrimaryTime();
                        day = primaryTime / 86400000;
                        secondaryTime = d._1().getSecondaryTime();
                        if(d._1().getValue() != -1){
                            datas.addDatum(newHost, team+metrics.get(0), primaryTime, secondaryTime, TimePartition.DAY, d._1().getValue());
                            count += 1;
                        }
                        if(d._2() != -1){
                            datas.addDatum(newHost, team+metrics.get(1), primaryTime, secondaryTime, TimePartition.DAY, d._2());
                            count += 1;
                        }
                        if(d._3() != -1){
                            datas.addDatum(newHost, team+metrics.get(2), primaryTime, secondaryTime, TimePartition.DAY, d._3());
                            count += 1;
                        }
                        if(d._4() != -1){
                            datas.addDatum(newHost, team+metrics.get(3), primaryTime, secondaryTime, TimePartition.DAY, d._4());
                            count += 1;
                        }
                        if(d._5() != -1){
                            datas.addDatum(newHost, team+metrics.get(4), primaryTime, secondaryTime, TimePartition.DAY, d._5());
                            count += 1;
                        }
                        if(d._6() != -1){
                            datas.addDatum(newHost, team+metrics.get(5), primaryTime, secondaryTime, TimePartition.DAY, d._6());
                            count += 1;
                        }
                        if(d._7() != -1){
                            datas.addDatum(newHost, team+metrics.get(6), primaryTime, secondaryTime, TimePartition.DAY, d._7());
                            count += 1;
                        }
                        if(d._8() != -1){
                            datas.addDatum(newHost, team+metrics.get(7), primaryTime, secondaryTime, TimePartition.DAY, d._8());
                            count += 1;
                        }if(d._9() != -1){
                            datas.addDatum(newHost, team+metrics.get(8), primaryTime, secondaryTime, TimePartition.DAY, d._9());
                            count += 1;
                        }
                        if(d._10() != -1){
                            datas.addDatum(newHost, team+metrics.get(9), primaryTime, secondaryTime, TimePartition.DAY, d._10());
                            count += 1;
                        }
                        if(d._11() != -1){
                            datas.addDatum(newHost, team+metrics.get(10), primaryTime, secondaryTime, TimePartition.DAY, d._11());
                            count += 1;
                        }
                        if(d._12() != -1){
                            datas.addDatum(newHost, team+metrics.get(11), primaryTime, secondaryTime, TimePartition.DAY, d._12());
                            count += 1;
                        }
                        if(d._13() != -1){
                            datas.addDatum(newHost, team+metrics.get(12), primaryTime, secondaryTime, TimePartition.DAY, d._13());
                            count += 1;
                        }
                        if(d._14() != -1){
                            datas.addDatum(newHost, team+metrics.get(13), primaryTime, secondaryTime, TimePartition.DAY, d._14());
                            count += 1;
                        }
                        if(d._15() != -1){
                            datas.addDatum(newHost, team+metrics.get(14), primaryTime, secondaryTime, TimePartition.DAY, d._15());
                            count += 1;
                        }
                        if(d._16() != -1){
                            datas.addDatum(newHost, team+metrics.get(15), primaryTime, secondaryTime, TimePartition.DAY, d._16());
                            count += 1;
                        }
                        if(d._17() != -1){
                            datas.addDatum(newHost, team+metrics.get(16), primaryTime, secondaryTime, TimePartition.DAY, d._17());
                            count += 1;
                        }
                        if(!d._18().equals("")){
                            datas.addDatum(newHost, team+metrics.get(17), primaryTime, secondaryTime, TimePartition.DAY, d._18());
                            count += 1;
                        }
                        if(d._19() != -1){
                            datas.addDatum(newHost, team+metrics.get(18), primaryTime, secondaryTime, TimePartition.DAY, d._19());
                            count += 1;
                        }
                        if(d._20() != -1){
                            datas.addDatum(newHost, team+metrics.get(19), primaryTime, secondaryTime, TimePartition.DAY, d._20());
                            count += 1;
                        }
                        if(d._21() != -1){
                            datas.addDatum(newHost, team+metrics.get(20), primaryTime, secondaryTime, TimePartition.DAY, d._21());
                            count += 1;
                        }
                        if(d._22() != -1){
                            datas.addDatum(newHost, team+metrics.get(21), primaryTime, secondaryTime, TimePartition.DAY, d._22());
                            count += 1;
                        }
                        if(day != lastDay){
                            throught += count;
                            try {
                                writer.bulkInsert(datas);
                            } catch (Exception e) {
                                e.getMessage();
                            }
                            count = 0;
                            lastDay = day;
                            datas = writer.newData();
                        }
                    }
                }
            }
        }
    }
}
