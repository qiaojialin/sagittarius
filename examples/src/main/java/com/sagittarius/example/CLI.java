package com.sagittarius.example;

import com.datastax.driver.core.Cluster;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.query.Shift;
import com.sagittarius.bean.result.*;
import com.sagittarius.core.SagittariusClient;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.read.SagittariusReader;
import com.sagittarius.util.TimeUtil;
import com.sagittarius.write.SagittariusWriter;
import com.sagittarius.write.Writer;
import com.sun.javafx.binding.StringFormatter;
import jline.console.*;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.System.exit;

/**
 * Created by MXW on 17-4-17.
 */
public class CLI {

    public static void main(String[] args) throws IOException, NoHostAvailableException, QueryExecutionException, TimeoutException, InterruptedException {
        if(args.length != 2){
            System.out.println("invalid arguments: spark master, kmx ip.");
            exit(0);
        }

        CassandraConnection connection = CassandraConnection.getInstance();
        Cluster cluster = connection.getCluster();
        SparkConf sparkConf = new SparkConf();
//        sparkConf.setMaster("spark://192.168.3.17:7077").setAppName("KMX_CLI");
        sparkConf.setMaster(args[1]).setAppName("KMX_CLI");
        //to fix the can't assign from .. to .. Error
        //String[] jars = {"examples-1.0-SNAPSHOT-jar-with-dependencies.jar"};
        //sparkConf.setJars(jars);
        sparkConf.set("spark.cassandra.connection.host", args[0]);
        sparkConf.set("spark.cassandra.connection.port", "9042");
        sparkConf.set("spark.cassandra.connection.keep_alive_ms", "600000");
        //sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //sparkConf.set("spark.kryoserializer.buffer.max", "512m");
        //sparkConf.set("spark.executor.extraJavaOptions", "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/agittarius/");
        //sparkConf.set("spark.scheduler.mode", "FAIR");
        //sparkConf.set("spark.executor.cores", "4");
        sparkConf.set("spark.cores.max", "20");
        //sparkConf.set("spark.driver.maxResultSize", "20g");
        //sparkConf.set("spark.driver.memory", "20g");
        sparkConf.set("spark.executor.memory", "2g");
        SagittariusClient client = new SagittariusClient(cluster, sparkConf, 10000);
        Writer writer = client.getWriter();
        SagittariusReader reader = (SagittariusReader)client.getReader();

        Thread.sleep(10000);
        ConsoleReader consoleReader = new ConsoleReader();
        MyCompleter myCompleter = new MyCompleter();
        consoleReader.addCompleter(myCompleter);
        String line = null;
        do
        {
            line = consoleReader.readLine("kmx>");
            if(line != null)
            {
                if(line.startsWith("get")){
                    String[] arguments = line.split(" ");
                    if(arguments[0].endsWith("Point")){
                        if(arguments.length == 4){
                            //getPrecise
                            String host = arguments[1];
                            ArrayList<String> hosts = new ArrayList<>();
                            String[] splitHosts = host.split(",");
                            for (String h : splitHosts){
                                hosts.add(h);
                            }
                            String metric = arguments[2];
                            ArrayList<String> metrics = new ArrayList<>();
                            String[] splitMetric = metric.split(",");
                            for(String m : splitMetric){
                                metrics.add(m);
                            }
                            long getTime;
                            try {
                                getTime = Long.valueOf(arguments[3]);
                            } catch (Exception e){
                                System.out.println("invalid time argument !");
                                continue;
                            }
                            if(arguments[0].equals("getIntPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, IntPoint>> result = null;
                                result = reader.getIntPoint(hosts, metrics, getTime);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10d", h, m, getTime, result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }

                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getLongPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, LongPoint>> result = null;
                                result = reader.getLongPoint(hosts, metrics, getTime);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10d", h, m, getTime, result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getFloatPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, FloatPoint>> result = null;
                                result = reader.getFloatPoint(hosts, metrics, getTime);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10f", h, m, getTime, result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getDoublePoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, DoublePoint>> result = null;
                                result = reader.getDoublePoint(hosts, metrics, getTime);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10f", h, m, getTime, result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getBooleanPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, BooleanPoint>> result = null;
                                result = reader.getBooleanPoint(hosts, metrics, getTime);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10s", h, m, getTime, result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getStringPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, StringPoint>> result = null;
                                result = reader.getStringPoint(hosts, metrics, getTime);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10s", h, m, getTime, result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getGeoPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, GeoPoint>> result = null;
                                result = reader.getGeoPoint(hosts, metrics, getTime);
                                System.out.println(String.format("%15s|%15s|%20s|%10s|%10s", "host", "metric", "primary_time", "latitude", "longitude"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10f|%10f", h, m, getTime, result.get(h).get(m).getLatitude(), result.get(h).get(m).getLongitude()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else {
                                System.out.println("invalid function !");
                            }
                        }
                        else if(arguments.length == 3){
                            //getLatest
                            String host = arguments[1];
                            ArrayList<String> hosts = new ArrayList<>();
                            String[] splitHosts = host.split(",");
                            for (String h : splitHosts){
                                hosts.add(h);
                            }
                            String metric = arguments[2];
                            ArrayList<String> metrics = new ArrayList<>();
                            String[] splitMetric = metric.split(",");
                            for(String m : splitMetric){
                                metrics.add(m);
                            }
                            if(arguments[0].equals("getIntPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, IntPoint>> result = null;
                                result = reader.getIntLatest(hosts, metrics);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10d", h, m, result.get(h).get(m).getPrimaryTime(), result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getLongPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, LongPoint>> result = null;
                                result = reader.getLongLatest(hosts, metrics);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10d", h, m, result.get(h).get(m).getPrimaryTime(), result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getFloatPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, FloatPoint>> result = null;
                                result = reader.getFloatLatest(hosts, metrics);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10f", h, m, result.get(h).get(m).getPrimaryTime(), result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getDoublePoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, DoublePoint>> result = null;
                                result = reader.getDoubleLatest(hosts, metrics);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10f", h, m, result.get(h).get(m).getPrimaryTime(), result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getBooleanPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, BooleanPoint>> result = null;
                                result = reader.getBooleanLatest(hosts, metrics);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10s", h, m, result.get(h).get(m).getPrimaryTime(), result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getStringPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, StringPoint>> result = null;
                                result = reader.getStringLatest(hosts, metrics);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10s", h, m, result.get(h).get(m).getPrimaryTime(), result.get(h).get(m).getValue()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getGeoPoint")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, GeoPoint>> result = null;
                                result = reader.getGeoLatest(hosts, metrics);
                                System.out.println(String.format("%15s|%15s|%20s|%10s|%10s", "host", "metric", "primary_time", "latitude", "longitude"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m : metrics){
                                            try{
                                                System.out.println(String.format("%15s|%15s|%20d|%10f|%10f", h, m, result.get(h).get(m).getPrimaryTime(), result.get(h).get(m).getLatitude(), result.get(h).get(m).getLongitude()));
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else {
                                System.out.println("invalid function !");
                            }
                        }
                        else if(arguments.length == 5){
                            //getFuzzy
                            String host = arguments[1];
                            String metric = arguments[2];
                            long getTime;
                            try {
                                getTime = Long.valueOf(arguments[3]);
                            } catch (Exception e){
                                System.out.println("invalid time argument !");
                                continue;
                            }
                            Shift shift = produceShiftFromArgs(arguments[4]);
                            if(shift == null){
                                System.out.println("invalid shift arguments!");
                                continue;
                            }
                            if(arguments[0].equals("getIntPoint")){
                                long timeStart = System.currentTimeMillis();
                                IntPoint result = reader.getFuzzyIntPoint(host, metric, getTime, shift);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null){
                                    System.out.println(String.format("%15s|%15s|%20d|%10d", host, metric, getTime, result.getValue()));
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getLongPoint")){
                                long timeStart = System.currentTimeMillis();
                                LongPoint result = reader.getFuzzyLongPoint(host, metric, getTime, shift);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null){
                                    System.out.println(String.format("%15s|%15s|%20d|%10d", host, metric, getTime, result.getValue()));
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getFloatPoint")){
                                long timeStart = System.currentTimeMillis();
                                FloatPoint result = reader.getFuzzyFloatPoint(host, metric, getTime, shift);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null){
                                    System.out.println(String.format("%15s|%15s|%20d|%10f", host, metric, getTime, result.getValue()));
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getDoublePoint")){
                                long timeStart = System.currentTimeMillis();
                                DoublePoint result = reader.getFuzzyDoublePoint(host, metric, getTime, shift);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null){
                                    System.out.println(String.format("%15s|%15s|%20d|%10f", host, metric, getTime, result.getValue()));
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getBooleanPoint")){
                                long timeStart = System.currentTimeMillis();
                                BooleanPoint result = reader.getFuzzyBooleanPoint(host, metric, getTime, shift);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null){
                                    System.out.println(String.format("%15s|%15s|%20d|%10s", host, metric, getTime, result.getValue()));
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getStringPoint")){
                                long timeStart = System.currentTimeMillis();
                                StringPoint result = reader.getFuzzyStringPoint(host, metric, getTime, shift);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null){
                                    System.out.println(String.format("%15s|%15s|%20d|%10s", host, metric, getTime, result.getValue()));
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getGeoPoint")){
                                long timeStart = System.currentTimeMillis();
                                GeoPoint result = reader.getFuzzyGeoPoint(host, metric, getTime, shift);
                                System.out.println(String.format("%15s|%15s|%20s|%10s|%10s", "host", "metric", "primary_time", "latitude", "longitude"));
                                if(result != null){
                                    System.out.println(String.format("%15s|%15s|%20d|%10f|%10f", host, metric, getTime, result.getLatitude(), result.getLongitude()));
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else {
                                System.out.println("invalid function !");
                            }
                        }
                        else {
                            System.out.println("wrong number of arguments !");
                        }
                    }
                    if(arguments[0].endsWith("Range")){
                        if(arguments.length == 5){
                            String host = arguments[1];
                            ArrayList<String> hosts = new ArrayList<>();
                            String[] splitHosts = host.split(",");
                            for (String h : splitHosts){
                                hosts.add(h);
                            }
                            String metric = arguments[2];
                            ArrayList<String> metrics = new ArrayList<>();
                            String[] splitMetric = metric.split(",");
                            for(String m : splitMetric){
                                metrics.add(m);
                            }
                            long primTime;
                            try {
                                primTime = Long.valueOf(arguments[3]);
                            } catch (Exception e){
                                System.out.println("invalid primary_time argument !");
                                continue;
                            }
                            long secoTime;
                            try {
                                secoTime = Long.valueOf(arguments[4]);
                            } catch (Exception e){
                                System.out.println("invalid secondary_time argument !");
                                continue;
                            }
                            if(arguments[0].equals("getIntRange")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, List<IntPoint>>> result = null;
                                result = reader.getIntRange(hosts, metrics, primTime, secoTime, false);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m:metrics){
                                            try{
                                                List<IntPoint> resultList = result.get(h).get(m);
                                                for(IntPoint r : resultList){
                                                    System.out.println(String.format("%15s|%15s|%20d|%10d", host, metric, r.getPrimaryTime(), r.getValue()));
                                                }
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getLongRange")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, List<LongPoint>>> result = null;
                                result = reader.getLongRange(hosts, metrics, primTime, secoTime, false);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m:metrics){
                                            try{
                                                List<LongPoint> resultList = result.get(h).get(m);
                                                for(LongPoint r : resultList){
                                                    System.out.println(String.format("%15s|%15s|%20d|%10d", host, metric, r.getPrimaryTime(), r.getValue()));
                                                }
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getFloatRange")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, List<FloatPoint>>> result = null;
                                result = reader.getFloatRange(hosts, metrics, primTime, secoTime, false);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m:metrics){
                                            try{
                                                List<FloatPoint> resultList = result.get(h).get(m);
                                                for(FloatPoint r : resultList){
                                                    System.out.println(String.format("%15s|%15s|%20d|%10f", host, metric, r.getPrimaryTime(), r.getValue()));
                                                }
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getDoubleRange")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, List<DoublePoint>>> result = null;
                                result = reader.getDoubleRange(hosts, metrics, primTime, secoTime, false);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m:metrics){
                                            try{
                                                List<DoublePoint> resultList = result.get(h).get(m);
                                                for(DoublePoint r : resultList){
                                                    System.out.println(String.format("%15s|%15s|%20d|%10f", host, metric, r.getPrimaryTime(), r.getValue()));
                                                }
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getBooleanRange")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, List<BooleanPoint>>> result = null;
                                result = reader.getBooleanRange(hosts, metrics, primTime, secoTime, false);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m:metrics){
                                            try{
                                                List<BooleanPoint> resultList = result.get(h).get(m);
                                                for(BooleanPoint r : resultList){
                                                    System.out.println(String.format("%15s|%15s|%20d|%10s", host, metric, r.getPrimaryTime(), r.getValue()));
                                                }
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getStringRange")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, List<StringPoint>>> result = null;
                                result = reader.getStringRange(hosts, metrics, primTime, secoTime, false);
                                System.out.println(String.format("%15s|%15s|%20s|%10s", "host", "metric", "primary_time", "value"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m:metrics){
                                            try{
                                                List<StringPoint> resultList = result.get(h).get(m);
                                                for(StringPoint r : resultList){
                                                    System.out.println(String.format("%15s|%15s|%20d|%10s", host, metric, r.getPrimaryTime(), r.getValue()));
                                                }
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else if(arguments[0].equals("getGeoRange")){
                                long timeStart = System.currentTimeMillis();
                                Map<String, Map<String, List<GeoPoint>>> result = null;
                                result = reader.getGeoRange(hosts, metrics, primTime, secoTime, false);
                                System.out.println(String.format("%15s|%15s|%20s|%10s|%10s", "host", "metric", "primary_time", "latitude", "longitude"));
                                if(result != null && !result.isEmpty()){
                                    for(String h : hosts){
                                        for(String m:metrics){
                                            try{
                                                List<GeoPoint> resultList = result.get(h).get(m);
                                                for(GeoPoint r : resultList){
                                                    System.out.println(String.format("%15s|%15s|%20d|%10f|%10f", host, metric, r.getPrimaryTime(), r.getLatitude(), r.getLongitude()));
                                                }
                                            } catch (Exception e){
                                                continue;
                                            }
                                        }
                                    }
                                }
                                timeStart = System.currentTimeMillis() - timeStart;
                                System.out.println("used time: " + timeStart + "ms");
                            }
                            else {
                                System.out.println("invalid function !");
                            }
                        }
                        else {
                            System.out.println("invalid number of arguments!");
                        }
                    }
                }
                if(line.startsWith("insert")){
                    String[] arguments = line.split(" ");
                    if(arguments.length == 7){
                        String host = arguments[1];
                        String metric = arguments[2];
                        long primTime;
                        try {
                            primTime = Long.valueOf(arguments[3]);
                        } catch (Exception e){
                            System.out.println("invalid primary_time !");
                            continue;
                        }
                        long secoTime;
                        try {
                            secoTime = Long.valueOf(arguments[4]);
                        } catch (Exception e){
                            System.out.println("invalid secondary_time !");
                            continue;
                        }
                        TimePartition timePartition = produceTimePartitionFromArgs(arguments[5]);
                        if(timePartition == null){
                            System.out.println("invalid TimePartition !");
                            continue;
                        }
                        if(arguments[0].equals("insertInt")){
                            int value;
                            try {
                                value = Integer.valueOf(arguments[6]);
                            } catch (Exception e){
                                System.out.println("invalid value !");
                                continue;
                            }
                            long timeStart = System.currentTimeMillis();
                            writer.insert(host, metric, primTime, secoTime, timePartition, value);
                            timeStart = System.currentTimeMillis() - timeStart;
                            System.out.println("insert successfully ! used time: " + timeStart + "ms");
                        }
                        else if(arguments[0].equals("insertLong")){
                            Long value;
                            try {
                                value = Long.valueOf(arguments[6]);
                            } catch (Exception e){
                                System.out.println("invalid value !");
                                continue;
                            }
                            long timeStart = System.currentTimeMillis();
                            writer.insert(host, metric, primTime, secoTime, timePartition, value);
                            timeStart = System.currentTimeMillis() - timeStart;
                            System.out.println("insert successfully ! used time: " + timeStart + "ms");
                        }
                        else if(arguments[0].equals("insertFloat")){
                            Float value;
                            try {
                                value = Float.valueOf(arguments[6]);
                            } catch (Exception e){
                                System.out.println("invalid value !");
                                continue;
                            }
                            long timeStart = System.currentTimeMillis();
                            writer.insert(host, metric, primTime, secoTime, timePartition, value);
                            timeStart = System.currentTimeMillis() - timeStart;
                            System.out.println("insert successfully ! used time: " + timeStart + "ms");
                        }
                        else if(arguments[0].equals("insertDouble")){
                            Double value;
                            try {
                                value = Double.valueOf(arguments[6]);
                            } catch (Exception e){
                                System.out.println("invalid value !");
                                continue;
                            }
                            long timeStart = System.currentTimeMillis();
                            writer.insert(host, metric, primTime, secoTime, timePartition, value);
                            timeStart = System.currentTimeMillis() - timeStart;
                            System.out.println("insert successfully ! used time: " + timeStart + "ms");
                        }
                        else if(arguments[0].equals("insertString")){
                            String value;
                            try {
                                value = String.valueOf(arguments[6]);
                            } catch (Exception e){
                                System.out.println("invalid value !");
                                continue;
                            }
                            long timeStart = System.currentTimeMillis();
                            writer.insert(host, metric, primTime, secoTime, timePartition, value);
                            timeStart = System.currentTimeMillis() - timeStart;
                            System.out.println("insert successfully ! used time: " + timeStart + "ms");
                        }
                        else if(arguments[0].equals("insertBoolean")){
                            Boolean value;
                            try {
                                value = Boolean.valueOf(arguments[6]);
                            } catch (Exception e){
                                System.out.println("invalid value !");
                                continue;
                            }
                            long timeStart = System.currentTimeMillis();
                            writer.insert(host, metric, primTime, secoTime, timePartition, value);
                            timeStart = System.currentTimeMillis() - timeStart;
                            System.out.println("insert successfully ! used time: " + timeStart + "ms");
                        }
                        else {
                            System.out.println("invalid function !");
                        }
                    }
                    else if(arguments.length == 8){
                        String host = arguments[1];
                        String metric = arguments[2];
                        long primTime;
                        try {
                            primTime = Long.valueOf(arguments[3]);
                        } catch (Exception e){
                            System.out.println("invalid primary_time !");
                            continue;
                        }
                        long secoTime;
                        try {
                            secoTime = Long.valueOf(arguments[4]);
                        } catch (Exception e){
                            System.out.println("invalid secondary_time !");
                            continue;
                        }
                        TimePartition timePartition = produceTimePartitionFromArgs(arguments[5]);
                        if(timePartition == null){
                            System.out.println("invalid TimePartition !");
                            continue;
                        }
                        if(arguments[0].equals("insertGeo")){
                            float lagitude;
                            float longitude;
                            try {
                                lagitude = Integer.valueOf(arguments[6]);
                            } catch (Exception e){
                                System.out.println("invalid lagitude !");
                                continue;
                            }
                            try {
                                longitude = Integer.valueOf(arguments[7]);
                            } catch (Exception e){
                                System.out.println("invalid longitude !");
                                continue;
                            }
                            long timeStart = System.currentTimeMillis();
                            writer.insert(host, metric, primTime, secoTime, timePartition, lagitude, longitude);
                            timeStart = System.currentTimeMillis() - timeStart;
                            System.out.println("insert successfully ! used time: " + timeStart + "ms");
                        }
                        else {
                            System.out.println("invalid function !");
                        }
                    }
                    else {
                        System.out.println("wrong number of arguments!");
                    }
                }
                if(line.startsWith("date")){
                    String[] arguments = line.split(" ", 2);
                    if(arguments[0].equalsIgnoreCase("date2long")){
                        try{
                            long dateTime = TimeUtil.string2Date(arguments[1]);
                            System.out.println(dateTime);
                        } catch (Exception e){
                            System.out.println("invalide date time!");
                            continue;
                        }
                    }
                }
            }
        }
        while(line!=null && !line.equals("exit"));
    }

    private static TimePartition produceTimePartitionFromArgs(String s){
        if(s.equalsIgnoreCase("Y")){
            return TimePartition.YEAR;
        }
        if(s.equalsIgnoreCase("M")){
            return TimePartition.MONTH;
        }
        if(s.equalsIgnoreCase("W")){
            return TimePartition.WEEK;
        }
        if(s.equalsIgnoreCase("D")){
            return TimePartition.DAY;
        }
        return null;
    }

    private static Shift produceShiftFromArgs(String s){
        if(s.equalsIgnoreCase("nearest")){
            return Shift.NEAREST;
        }
        if(s.equalsIgnoreCase("after")){
            return Shift.AFTER;
        }
        if(s.equalsIgnoreCase("before")){
            return Shift.BEFORE;
        }
        return null;
    }
}
