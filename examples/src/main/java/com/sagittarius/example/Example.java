package com.sagittarius.example;

import com.datastax.driver.core.Cluster;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.datastax.spark.connector.util.Symbols;
import com.sagittarius.bean.common.MetricMetadata;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.AggregationType;
import com.sagittarius.bean.query.Shift;
import com.sagittarius.bean.result.DoublePoint;
import com.sagittarius.bean.result.FloatPoint;
import com.sagittarius.bean.result.IntPoint;
import com.sagittarius.bean.result.StringPoint;
import com.sagittarius.core.SagittariusClient;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.SparkException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.read.Reader;
import com.sagittarius.read.SagittariusReader;
import com.sagittarius.util.TimeUtil;
import com.sagittarius.write.SagittariusWriter;
import com.sagittarius.write.Writer;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple22;
import scala.tools.cmd.gen.AnyVals;

import java.io.*;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static java.lang.System.exit;
import static java.lang.System.setOut;


public class Example {
    private static final Logger logger = LoggerFactory.getLogger(Example.class);

    public static void main(String[] args) throws IOException, QueryExecutionException, TimeoutException, NoHostAvailableException, ParseException, SparkException {
        CassandraConnection connection = CassandraConnection.getInstance();
        Cluster cluster = connection.getCluster();

        SparkConf sparkConf = new SparkConf();
//        sparkConf.setMaster("spark://192.168.3.17:7077").setAppName("test");
        sparkConf.setMaster("spark://192.168.15.114:7077").setAppName("kmxtest");
        //to fix the can't assign from .. to .. Error
        String[] jars = {"examples-1.0-SNAPSHOT-jar-with-dependencies.jar"};
        sparkConf.setJars(jars);
        sparkConf.set("spark.cassandra.connection.host", "192.168.15.120");
        sparkConf.set("spark.cassandra.connection.port", "9042");
        sparkConf.set("spark.cassandra.connection.keep_alive_ms", "600000");
//        sparkConf.set("spark.driver.host","192.168.15.123");

        //sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //sparkConf.set("spark.kryoserializer.buffer.max", "512m");
        //sparkConf.set("spark.executor.extraJavaOptions", "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/agittarius/");
        //sparkConf.set("spark.scheduler.mode", "FAIR");
        //sparkConf.set("spark.executor.cores", "4");
        sparkConf.set("spark.cores.max", "20");
        //sparkConf.set("spark.driver.maxResultSize", "20g");
        //sparkConf.set("spark.driver.memory", "20g");
//        sparkConf.set("spark.executor.memory", "2g");
        SagittariusClient client = new SagittariusClient(cluster, sparkConf, 10000);
        SagittariusWriter writer = (SagittariusWriter) client.getWriter();
        SagittariusReader reader = (SagittariusReader)client.getReader();
//        ReadTask task1 = new ReadTask(reader, time, "value >= 33 and value <= 34");
//        ReadTask task2 = new ReadTask(reader, time, "value >= 34 and value <= 35");
//        ReadTask task3 = new ReadTask(reader, time, "value >= 35 and value <= 36");
        //batchTest(writer, Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        //batchTest1(client, Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        //insert(writer);
        //task1.start();
        //task2.start();
        //task3.start();
        //test(client.getSparkContext());
        //floatRead(reader);
        insert(writer);
//        TestTask testTask = new TestTask(reader);
//        testTask.start();
        //floatRead(reader);
        //logger.info("consume time: " + (System.currentTimeMillis() - time) + "ms");
        //registerHostMetricInfo(writer);
        //registerHostTags(writer);
        //registerOwnerInfo(writer);
        //insert(writer);
        //writeTest(writer, Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        //batchWriteTest(writer, Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
//        batchWriteTest(writer, 1, 1, 1000);
        //insertLoop(writer);

        //read(reader);
        //readbyRange(reader);
        //readFuzzy(reader);
//        long st = System.currentTimeMillis();
//        floatRead(reader);
//        System.out.println(System.currentTimeMillis() - st);
//        test(reader);
//        int threads = Integer.valueOf(args[0]);
//        int batchSize = Integer.valueOf(args[1]);
//        String directory = args[2];
//        batchWriteBigData2(writer, threads, batchSize, directory);

//        deleteTest(reader, writer);
        exit(0);

    }

    private static void deleteTest(SagittariusReader reader, SagittariusWriter writer) throws ParseException, NoHostAvailableException, QueryExecutionException, TimeoutException, SparkException {
        ArrayList<String> hosts = new ArrayList<>();
        hosts.add("128934");
//        hosts.add("128579");
//        hosts.add("130889");
//        hosts.add("131979");
//        hosts.add("135931");
//        hosts.add("139307");
//        hosts.add("129007");
//        hosts.add("130894");
//        hosts.add("130966");
//        hosts.add("131967");
//        hosts.add("133678");
//        hosts.add("134738");
//        hosts.add("135896");
//        hosts.add("135922");
//        hosts.add("139025");
//        hosts.add("139334");
        ArrayList<String> metrics = new ArrayList<>();
//        metrics.add("加速踏板位置1");
//        metrics.add("当前转速下发动机负载百分比");
//        metrics.add("实际发动机扭矩百分比");
//        metrics.add("发动机转速");
//        metrics.add("高精度总里程(00)");
//        metrics.add("总发动机怠速使用燃料");
//        metrics.add("后处理1排气质量流率");
//        metrics.add("总发动机操作时间");
//        metrics.add("总发动机使用的燃油");
//        metrics.add("发动机冷却液温度");
//        metrics.add("基于车轮的车辆速度");
//        metrics.add("发动机燃料消耗率");
        metrics.add("大气压力");
//        metrics.add("发动机进气歧管1压力");
//        metrics.add("发动机进气歧管1温度");
//        metrics.add("发动机进气压力");
//        metrics.add("车速");
//        metrics.add("大气温度");
//        metrics.add("发动机进气温度");
//        metrics.add("高精度总里程(EE)");
//        metrics.add("后处理1进气氮氧化物浓度");
        long startTime = TimeUtil.string2Date("2016-06-02 15:00:00");
        long endTime = TimeUtil.string2Date("2016-06-05 12:00:00");
//        1464840000000
//        1464883200000
//        1465099200000
//        1465056000000
//        reader.preAggregateFunction(hosts, metrics, startTime, endTime, writer);
//        Map<String, Map<String, List<FloatPoint>>> result5 = reader.getFloatRange(hosts, metrics, startTime,endTime,false);
//        Map<String, Map<String, Double>> result = reader.getFloatRange(hosts, metrics, startTime, endTime, null, AggregationType.COUNT);
//        Map<String, Map<String, Double>> result3 = reader.getFloatRange(hosts, metrics, startTime, endTime, null, AggregationType.COUNT);
//        Map<String, Map<String, Double>> result2 = reader.getAggregationRange(hosts, metrics, startTime, endTime, null, AggregationType.COUNT);
//        System.out.println(result.get(hosts.get(0)).get(metrics.get(0)));
//        System.out.println(result3.get(hosts.get(0)).get(metrics.get(0)));
//        System.out.println(result5.get(hosts.get(0)).get(metrics.get(0)).size());
    }

    private static void preAggTest(SagittariusReader reader, SagittariusWriter writer) throws ParseException, QueryExecutionException, TimeoutException, NoHostAvailableException, SparkException, IOException {
        ArrayList<String> hosts = new ArrayList<>();
        hosts.add("128934");
//        hosts.add("128579");
//        hosts.add("130889");
//        hosts.add("131979");
//        hosts.add("135931");
//        hosts.add("139307");
//        hosts.add("129007");
//        hosts.add("130894");
//        hosts.add("130966");
//        hosts.add("131967");
//        hosts.add("133678");
//        hosts.add("134738");
//        hosts.add("135896");
//        hosts.add("135922");
//        hosts.add("139025");
//        hosts.add("139334");
        ArrayList<String> metrics = new ArrayList<>();
        metrics.add("加速踏板位置1");
        metrics.add("当前转速下发动机负载百分比");
        metrics.add("实际发动机扭矩百分比");
        metrics.add("发动机转速");
        metrics.add("高精度总里程(00)");
        metrics.add("总发动机怠速使用燃料");
        metrics.add("后处理1排气质量流率");
        metrics.add("总发动机操作时间");
        metrics.add("总发动机使用的燃油");
        metrics.add("发动机冷却液温度");
        metrics.add("基于车轮的车辆速度");
        metrics.add("发动机燃料消耗率");
        metrics.add("大气压力");
        metrics.add("发动机进气歧管1压力");
        metrics.add("发动机进气歧管1温度");
        metrics.add("发动机进气压力");
        metrics.add("车速");
        metrics.add("大气温度");
        metrics.add("发动机进气温度");
        metrics.add("高精度总里程(EE)");
        metrics.add("后处理1进气氮氧化物浓度");
        long startTime = TimeUtil.string2Date("2016-06-01 00:00:00");
        long endTime = TimeUtil.string2Date("2016-06-08 00:00:00");
        FileWriter fileWritter = null;
        try {
            fileWritter = new FileWriter("D:\\ty\\PreAggTestResult" , true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
        List<String> queryHosts = new ArrayList<>();
        for(String host : hosts){
            queryHosts.add(host);
            List<String> queryMetric = new ArrayList<>();
            for(String metric : metrics){
                queryMetric.add(metric);
                for(long i = 1; i < 31; i++){
                    long day = (long)(Math.random()*50);
                    System.out.println(day);
                    long queryStartTime = startTime + day * 86400000L;
                    long queryEndTime = queryStartTime + i * 86400000L;
                    long timer = System.currentTimeMillis();
                    reader.preAggregateFunction(queryHosts, queryMetric, queryStartTime, queryEndTime, writer);
//        Map<String, Map<String, Double>> result = reader.getFloatRange(hosts, metrics, startTime, endTime, null, AggregationType.COUNT);
//        Map<String, Map<String, Double>> result = reader.getAggregationRange(hosts, metrics, startTime, endTime, null, AggregationType.COUNT, ValueType.FLOAT);
                    timer = System.currentTimeMillis() - timer;
                    bufferWritter.write(queryHosts.size()*queryMetric.size()*i + ","+ timer + "\n");
                    bufferWritter.flush();
                }
            }
        }
//        System.out.println(startTime/86400000);
//        System.out.println(result == null);
//        for(String m : metrics){
//            System.out.println(result.get(hosts.get(0)).get(m));
//        }
    }

    public static void test(Reader reader) {
        ArrayList<String> hosts = new ArrayList<>();
        hosts.add("128934");
        ArrayList<String> metrics = new ArrayList<>();
        metrics.add("当前转速下发动机负载百分比");
        metrics.add("实际发动机扭矩百分比");
        metrics.add("发动机转速");
        metrics.add("高精度总里程(00)");
        metrics.add("总发动机怠速使用燃料");

    }

    private static void IntRead(Reader reader){
        ArrayList<String> hosts = new ArrayList<>();
        hosts.add("clihost");
        ArrayList<String> metrics = new ArrayList<>();
        metrics.add("climetric");
        Map<String, Map<String, IntPoint>> result = null;
        try {
            result = reader.getIntPoint(hosts, metrics, 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(result.get("clihost").get("climetric").getValue());
    }

    private static void floatRead(Reader reader) throws ParseException {
        ArrayList<String> hosts = new ArrayList<>();
        hosts.add("128998");
        ArrayList<String> metrics = new ArrayList<>();
        metrics.add("发动机转速");
        long start = TimeUtil.string2Date("2016-06-01 00:00:00");
        long end = TimeUtil.string2Date("2016-10-01 00:00:00");
        String filter = "value >= 33 and value <= 34";
        Map<String, Map<String, Double>> result = null;
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        try {
//            result = reader.getFloatRange(hosts, metrics, start, end, null, AggregationType.COUNT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(result.get("128998").get("发动机转速"));
    }

    private static void batchTest1(SagittariusClient client, int threads, int runTime) {
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < threads; ++i) {
            hosts.add("12828" + i);
        }
        List<BatchTest> tasks = new ArrayList<>();
        for (String host : hosts) {
            BatchTest task = new BatchTest(client.getWriter(), host, runTime);
            task.start();
            tasks.add(task);
        }

        long start = System.currentTimeMillis();
        long count = 0;
        long consume;
        while (true) {
            try {
                Thread.sleep(180000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (BatchTest task : tasks) {
                count += task.getCount();
            }
            consume = System.currentTimeMillis() - start;
            double throughput = count / ((double) consume / 1000);

            logger.info("throughput: " + throughput + ", count: " + count);
            count = 0;
        }
    }

    private static void batchTest(Writer writer, int threads, int runTime) {
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < threads; ++i) {
            hosts.add("12828" + i);
        }
        List<BatchTest> tasks = new ArrayList<>();
        for (String host : hosts) {
            BatchTest task = new BatchTest(writer, host, runTime);
            task.start();
            tasks.add(task);
        }

        long start = System.currentTimeMillis();
        long count = 0;
        long consume;
        while (true) {
            try {
                Thread.sleep(180000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (BatchTest task : tasks) {
                count += task.getCount();
            }
            consume = System.currentTimeMillis() - start;
            double throughput = count / ((double) consume / 1000);
            count = 0;
            logger.info("throughput: " + throughput + ", count: " + count);
        }
    }

    private static void batchWriteTest(Writer writer, int threads, int runTime, int batchSize) {
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < threads; ++i) {
            hosts.add("12828" + i);
        }
        List<BatchWriteTask> tasks = new ArrayList<>();
        for (String host : hosts) {
            BatchWriteTask task = new BatchWriteTask(writer, host, runTime, batchSize);
            task.start();
            tasks.add(task);
        }

        while (true) {
            try {
                Thread.sleep(180000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            double throughput = 0;
            long count = 0;
            for (BatchWriteTask task : tasks) {
                throughput += task.getThroughput();
                count += task.getCount();
            }
            logger.info("throughput: " + throughput + ", count: " + count);
        }
    }

    private static void writeTest(Writer writer, int threads, int runTime) {
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < threads; ++i) {
            hosts.add("12828" + i);
        }
        List<WriteTask> tasks = new ArrayList<>();
        for (String host : hosts) {
            WriteTask task = new WriteTask(writer, host, runTime);
            task.start();
            tasks.add(task);
        }

        /*final LoadBalancingPolicy loadBalancingPolicy =
                cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
        final PoolingOptions poolingOptions =
                cluster.getConfiguration().getPoolingOptions();

        ScheduledExecutorService scheduled =
                Executors.newScheduledThreadPool(1);
        scheduled.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Session.State state = client.getSession().getState();
                for (Host host : state.getConnectedHosts()) {
                    HostDistance distance = loadBalancingPolicy.distance(host);
                    int connections = state.getOpenConnections(host);
                    int inFlightQueries = state.getInFlightQueries(host);
                    System.out.printf("%s connections=%d, current load=%d, maxload=%d%n",
                            host, connections, inFlightQueries,
                            connections * poolingOptions.getMaxRequestsPerConnection(distance));
                }
            }
        }, 5, 5, TimeUnit.SECONDS);*/

        while (true) {
            try {
                Thread.sleep(180000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            double throughput = 0;
            long count = 0;
            for (WriteTask task : tasks) {
                throughput += task.getThroughput();
                count += task.getCount();
            }
            logger.info("throughput: " + throughput + ", count: " + count);
        }
    }

    private static void read(Reader reader) {
        List<String> hosts = new ArrayList<>();
        hosts.add("128280");
        hosts.add("128290");
        List<String> metrics = new ArrayList<>();
        metrics.add("APP");
        Map<String, Map<String, DoublePoint>> result = null;
        try {
            result = reader.getDoublePoint(hosts, metrics, 1482319512851L);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (Map.Entry<String, Map<String, DoublePoint>> entry : result.entrySet()) {
            //
        }
    }
    private static void readLatest(Reader reader) {
        List<String> hosts = new ArrayList<>();
        hosts.add("128280");
        hosts.add("128290");
        List<String> metrics = new ArrayList<>();
        metrics.add("APP");
        Map<String, Map<String, DoublePoint>> result = null;
        try {
            result = reader.getDoubleLatest(hosts, metrics);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (Map.Entry<String, Map<String, DoublePoint>> entry : result.entrySet()) {
            //
        }
    }
    private static void readbyRange(Reader reader) {
        List<String> hosts = new ArrayList<>();
        //hosts.add("131980");
        System.out.println("查找");
        hosts.add("128280");
        hosts.add("1282835");
        List<String> metrics = new ArrayList<>();
        metrics.add("APP");
        LocalDateTime start = LocalDateTime.of(1993,10,11,0,0);
        LocalDateTime end = LocalDateTime.of(1993,10,14,5,59);
        Map<String, Map<String, List<DoublePoint>>> result = null;
        try {
            result = reader.getDoubleRange(hosts, metrics,start.toEpochSecond(TimeUtil.zoneOffset)*1000,end.toEpochSecond(TimeUtil.zoneOffset)*1000, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (Map.Entry<String, Map<String, List<DoublePoint>>> entry : result.entrySet()) {
            //
        }
    }

    private static void readFuzzy(Reader reader) {

        String host="128280";
        String metric="APP";
        DoublePoint point = null;
        try {
            point = reader.getFuzzyDoublePoint(host,metric,1483712410000L, Shift.NEAREST);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertLoop(Writer writer) {
        LocalDateTime start = LocalDateTime.of(1993,10,12,0,0);
        LocalDateTime end = LocalDateTime.of(1993,10,14,23,59);
        System.out.println("插入");
        while (!start.isAfter(end)) {
            double value=Math.random()*100;
            try {
                writer.insert("1282835", "APP", start.toEpochSecond(TimeUtil.zoneOffset)*1000, start.toEpochSecond(TimeUtil.zoneOffset)*1000, TimePartition.DAY,value );
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("APP" + " " + start.toEpochSecond(TimeUtil.zoneOffset)*1000+" "+ start.toString()+ " " + value);
            start = start.plusHours(6);
        }
    }

    private static void insert(Writer writer) {
        long time1 = System.currentTimeMillis();
        long time2 = System.currentTimeMillis();
        System.out.println(time1);
        System.out.println(time2);
        //for (int i = 0; i < 3000; ++i) {
        try {
            writer.insert("128280", "APP", time1, -1, TimePartition.DAY, "test");
        } catch (Exception e) {
            e.printStackTrace();
        }
        ++time1;
        //}
        logger.info("" + (System.currentTimeMillis() - time1));
        /*long start = System.currentTimeMillis();
        SagittariusWriter sWriter = (SagittariusWriter)writer;
        SagittariusWriter.Datas datas = sWriter.newDatas();
        long time = System.currentTimeMillis();
        Random random = new Random();
        for (int i = 0; i < 3000; ++i) {
            datas.addData("128280", "APP", time, time, TimePartition.DAY, random.nextDouble() * 100);
            ++time;
        }
        sWriter.bulkInsert(datas);
        logger.info(" " + (System.currentTimeMillis() - start));*/
    }

    private static void registerHostMetricInfo(Writer writer) {
        MetricMetadata metricMetadata1 = new MetricMetadata("APP", TimePartition.DAY, ValueType.DOUBLE, "加速踏板位置");
        MetricMetadata metricMetadata2 = new MetricMetadata("ECT", TimePartition.DAY, ValueType.INT, "发动机冷却液温度");
        List<MetricMetadata> metricMetadatas = new ArrayList<>();
        metricMetadatas.add(metricMetadata1);
        metricMetadatas.add(metricMetadata2);
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < 50; ++i) {
            hosts.add("12828" + i);
        }
        for (String host : hosts) {
            long time = System.currentTimeMillis();
            try {
                writer.registerHostMetricInfo(host, metricMetadatas);
            } catch (Exception e) {
                e.printStackTrace();
            }
            logger.info("consume time: " + (System.currentTimeMillis() - time) + "ms");
        }
    }

    private static void registerHostTags(Writer writer) {
        Map<String, String> tags = new HashMap<>();
        tags.put("price", "¥.10000");
        try {
            writer.registerHostTags("128280", tags);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static float myParseFloat(String d){
        if(d.equals("")){
            return -1;
        }
        else {
            return Float.parseFloat(d);
        }
    }

//    private static void batchWriteBigData(SagittariusWriter writer, int threads, int batchSize, String directoryPath) throws IOException {
//
//        List<String> metrics = new ArrayList<>();
//        metrics.add("加速踏板位置1");
//        metrics.add("当前转速下发动机负载百分比");
//        metrics.add("实际发动机扭矩百分比");
//        metrics.add("发动机转速");
//        metrics.add("高精度总里程(00)");
//        metrics.add("总发动机怠速使用燃料");
//        metrics.add("后处理1排气质量流率");
//        metrics.add("总发动机操作时间");
//        metrics.add("总发动机使用的燃油");
//        metrics.add("发动机冷却液温度");
//        metrics.add("基于车轮的车辆速度");
//        metrics.add("发动机燃料消耗率");
//        metrics.add("大气压力");
//        metrics.add("发动机进气歧管1压力");
//        metrics.add("发动机进气歧管1温度");
//        metrics.add("发动机进气压力");
//        metrics.add("车速");
//        metrics.add("发动机扭矩模式");
//        metrics.add("大气温度");
//        metrics.add("发动机进气温度");
//        metrics.add("高精度总里程(EE)");
//        metrics.add("后处理1进气氮氧化物浓度");
//
//        //a map to store all datas
//        HashMap<String, ArrayList<Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float>>> map = new HashMap<>();
//
//        //get all data files
//        File[] fileList = new File(directoryPath).listFiles(new FileFilter() {
//            @Override
//            public boolean accept(File pathname) {
//                if (pathname.getName().endsWith(".csv"))
//                    return true;
//                return false;
//            }
//        });
//        System.out.println("the number of files = " + fileList.length);
//
//        //put every file's data into map
//        for (File file : fileList){
//            BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
//            //skip the first line
//            String line = bf.readLine();
//            String host = "";
//            //array to store data lines
//            ArrayList<Tuple22<Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float>> rowDataSets = new ArrayList<>();
//            while ((line = bf.readLine()) != null){
//                String[] row = line.split(",", -1);
//                host = row[0];
//                try {
//                    long primaryTime = TimeUtil.string2Date(row[1], TimeUtil.dateFormat1);
//                    long secondaryTime = TimeUtil.string2Date(row[2], TimeUtil.dateFormat1);
//                } catch (Exception e) {
//                    continue;
//                }
//
//                Tuple22<Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float> rowData =
//                        new Tuple22<>(myParseFloat(row[3]),myParseFloat(row[4]),myParseFloat(row[5]),myParseFloat(row[6]),myParseFloat(row[7]),
//                                myParseFloat(row[8]),myParseFloat(row[9]),myParseFloat(row[10]),myParseFloat(row[11]),
//                                myParseFloat(row[12]),myParseFloat(row[13]),myParseFloat(row[14]),myParseFloat(row[15]),
//                                myParseFloat(row[16]),myParseFloat(row[17]),myParseFloat(row[18]),myParseFloat(row[19]),
//                                row[20],myParseFloat(row[21]),myParseFloat(row[22]),myParseFloat(row[23]),myParseFloat(row[24]));
//                rowDataSets.add(rowData);
//            }
//            if(map.containsKey(host)){
//                rowDataSets.addAll(map.get(host));
//                map.put(host,rowDataSets);
//            }
//            else {
//                map.put(host,rowDataSets);
//            }
//        }
//
//        System.out.println("put all data into map");
//        Set<String> hosts = map.keySet();
//        System.out.println("the number of hosts = " + hosts.size());
//        List<BatchWriteBigDataTask> tasks = new ArrayList<>();
//        long start = System.currentTimeMillis();
//        for(int i = 0; i < threads; i++){
//            BatchWriteBigDataTask task = new BatchWriteBigDataTask(writer, i, batchSize, metrics, map);
//            task.start();
//            tasks.add(task);
//        }
//        long end = System.currentTimeMillis();
//        System.out.println("time : " + (end-start));
//
//        long sumer = 0;
//        while (true){
//            try {
//                Thread.sleep(60000);
//            }catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            for (BatchWriteBigDataTask task : tasks) {
//                sumer += task.getThrought();
//            }
//            System.out.println(sumer/60);
//            sumer = 0;
//        }
//    }

    private static void batchWriteBigData2(SagittariusWriter writer, int threads, int batchSize, String directoryPath) throws IOException {

        List<String> metrics = new ArrayList<>();
        metrics.add("加速踏板位置1");
        metrics.add("当前转速下发动机负载百分比");
        metrics.add("实际发动机扭矩百分比");
        metrics.add("发动机转速");
        metrics.add("高精度总里程(00)");
        metrics.add("总发动机怠速使用燃料");
        metrics.add("后处理1排气质量流率");
        metrics.add("总发动机操作时间");
        metrics.add("总发动机使用的燃油");
        metrics.add("发动机冷却液温度");
        metrics.add("基于车轮的车辆速度");
        metrics.add("发动机燃料消耗率");
        metrics.add("大气压力");
        metrics.add("发动机进气歧管1压力");
        metrics.add("发动机进气歧管1温度");
        metrics.add("发动机进气压力");
        metrics.add("车速");
        metrics.add("发动机扭矩模式");
        metrics.add("大气温度");
        metrics.add("发动机进气温度");
        metrics.add("高精度总里程(EE)");
        metrics.add("后处理1进气氮氧化物浓度");

        //a map to store all datas
        HashMap<String, ArrayList<Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float>>> map = new HashMap<>();

        //get all data files
        File[] fileList = new File(directoryPath).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.getName().endsWith(".csv"))
                    return true;
                return false;
            }
        });
        System.out.println("the number of files = " + fileList.length);

        //put every file's data into map
        for (File file : fileList){
            BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            //skip the first line
            String line = bf.readLine();
            String host = "";
            //array to store data lines
            ArrayList<Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float>> rowDataSets = new ArrayList<>();
            while ((line = bf.readLine()) != null){
                String[] row = line.split(",", -1);
                host = row[0];
                long primaryTime, secondaryTime;
                try {
                    primaryTime = TimeUtil.string2Date(row[1]);
                    secondaryTime = TimeUtil.string2Date(row[2]);
                } catch (Exception e) {
                    System.out.println(file.getCanonicalPath());
                    continue;
                }

                Tuple22<FloatPoint,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,Float,String,Float,Float,Float,Float> rowData =
                        new Tuple22<>(
                                new FloatPoint(metrics.get(0), primaryTime, secondaryTime, myParseFloat(row[3])),
                                myParseFloat(row[4]),
                                myParseFloat(row[5]),
                                myParseFloat(row[6]),
                                myParseFloat(row[7]),
                                myParseFloat(row[8]),
                                myParseFloat(row[9]),
                                myParseFloat(row[10]),
                                myParseFloat(row[11]),
                                myParseFloat(row[12]),
                                myParseFloat(row[13]),
                                myParseFloat(row[14]),
                                myParseFloat(row[15]),
                                myParseFloat(row[16]),
                                myParseFloat(row[17]),
                                myParseFloat(row[18]),
                                myParseFloat(row[19]),
                                row[20],
                                myParseFloat(row[21]),
                                myParseFloat(row[22]),
                                myParseFloat(row[23]),
                                myParseFloat(row[24]));
                rowDataSets.add(rowData);
            }
            if(map.containsKey(host)){
                rowDataSets.addAll(map.get(host));
                map.put(host,rowDataSets);
            }
            else {
                map.put(host,rowDataSets);
            }
        }

        System.out.println("put all data into map");
        Set<String> hosts = map.keySet();
        System.out.println("the number of hosts = " + hosts.size());
        List<BatchWriteBigDataTask> tasks = new ArrayList<>();
        long start = System.currentTimeMillis();
        for(int i = 0; i < threads; i++){
            BatchWriteBigDataTask task = new BatchWriteBigDataTask(writer, i, batchSize, metrics, map);
            task.start();
            tasks.add(task);
        }
        long end = System.currentTimeMillis();
        System.out.println("time : " + (end-start));

        long sumer = 0;
        long recorder = sumer;
        while (true){
            try {
                Thread.sleep(60000);
            }catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (BatchWriteBigDataTask task : tasks) {
                sumer += task.getThrought();
            }
            System.out.println("whentimeis " + TimeUtil.date2String(System.currentTimeMillis(), TimeUtil.dateFormat1) + ", " + (sumer-recorder)/60);
            recorder = sumer;
            sumer = 0;
        }
    }

    private static void batchWriteBigData3(SagittariusWriter writer, int threads, int batchSize, String directoryPath) throws IOException {

        //to compute the max number of frames devices sent to kmx per second

        List<Long> times = new ArrayList<>();
        //get all data files
        File[] fileList = new File(directoryPath).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.getName().endsWith(".csv"))
                    return true;
                return false;
            }
        });
        System.out.println("the number of files = " + fileList.length);
        long time1 = 0;
        try {
            time1 = TimeUtil.string2Date("2016-06-02 00:00:00");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long time2 = 0;
        try {
            time2 = TimeUtil.string2Date("2016-06-05 00:00:00");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //put every file's data into map
        for (File file : fileList){
            BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            //skip the first line
            String line = bf.readLine();

            //array to store data lines
             while ((line = bf.readLine()) != null) {
                String[] row = line.split(",", -1);
                long primaryTime;
                try {
                    primaryTime = TimeUtil.string2Date(row[1]);
                } catch (Exception e) {
                    System.out.println(file.getCanonicalPath());
                    continue;
                }
                if(primaryTime >= time1 && primaryTime < time2){
                    times.add(primaryTime);
                }
            }



        }

        System.out.println(times.size());

        times.sort(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1.compareTo(o2);
            }
        });

        long temp = 0;
        long max = 0;
        long tempTime = times.get(0);
        for(int i = 1; i < times.size(); i++){
            if(times.get(i) == tempTime){
                temp += 1;
            }
            else {
                tempTime = times.get(i);
                max = temp > max ? temp : max;
                temp = 0;
            }
        }

        System.out.println(max);

        System.out.println("put all data into map");

    }
}
