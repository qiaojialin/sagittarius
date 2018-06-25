package com.sagittarius.read;

import com.datastax.driver.core.Cluster;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.result.FloatPoint;
import com.sagittarius.core.SagittariusClient;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.net.UnknownHostException;
import java.util.*;

public class test {
    public static void main(String[] args) throws UnknownHostException, NoHostAvailableException, QueryExecutionException, TimeoutException {
        Cluster cluster;
        String[] CONTACT_POINTS = {"192.168.3.151","192.168.3.152"};
        cluster = Cluster.builder()
                .addContactPoints(CONTACT_POINTS).withPort(9042)
                .build();
        SagittariusClient client = new SagittariusClient(cluster, 10000);

        List<String> devices = new ArrayList<>();
        devices.add("1001140474");
        List<String> metrics = new ArrayList<>();
        metrics.add("J_0001_00_110");
        metrics.add("J_0001_00_602");
        metrics.add("J_0001_00_3667");
        Map<ValueType, Map<String, Set<String>>> valueTypeMapMap = client.getReader().getValueType(devices, metrics);
        SagittariusReader reader = (SagittariusReader) client.getReader();
        for (Map.Entry entry : valueTypeMapMap.entrySet()) {
            ValueType valueType = (ValueType) entry.getKey();
            Map<String, Set<String>> devices_metrics = (HashMap<String, Set<String>>) entry.getValue();
            List<String> group_devices = new ArrayList<>(devices_metrics.get("hosts"));
            List<String> group_metrics = new ArrayList<>(devices_metrics.get("metrics"));
            List<String> sqls = reader.getRangeQueryString(group_devices, group_metrics, 1493199532000L, 1498642732000L, valueType, true);
//            System.out.println(reader.getFloatRange(group_devices, group_metrics, 1493199532000L, 1498642732000L, true));
//            Map<String, Map<String, List<FloatPoint>>> stringMapMap = reader.getFloatRange(group_devices, group_metrics, 1493199532000L, 1498642732000L, true);
//            for(Map.Entry entry1: stringMapMap.entrySet()) {
//                for(Map.Entry entry2: ((Map<String, List<FloatPoint>>) entry1.getValue()).entrySet()) {
//                    List<FloatPoint> floatPoints = (List<FloatPoint>)entry2.getValue();
//                    for(FloatPoint point: floatPoints) {
//                        System.out.println(point.getValue());
//                    }
//                }
//            }
            System.out.println(sqls);
            for(String sql: sqls) {
                sql = sql.replace("select * from ", "select * from sagittarius.");
                System.out.println(sql);

                String path = sql;
                SparkSession sparkSessionNew = SparkSession.builder().config("spark.master", "local").config("spark.cassandra.connection.host", "192.168.3.151").getOrCreate();
                String filter = path;

                //parse keyspace and table
                int indexFrom = path.indexOf("from");
                path = path.substring(indexFrom + 4, path.length());
                while (path.charAt(0) == ' ') {
                    path = path.substring(1, path.length());
                }
                int indexSemi = path.indexOf(";");
                if (indexSemi != -1) {
                    path = path.substring(0, indexSemi);
                }
                int indexSpace = path.indexOf(" ");
                if (indexSpace != -1) {
                    path = path.substring(0, indexSpace);
                }
                String[] splits = path.split("\\.");
                String keyspace = splits[0];
                String table = splits[1];

                //set options
                Map<String, String> options = new HashMap<>();
                options.put("keyspace", keyspace);
                options.put("table", table);
                Dataset<Row> df = sparkSessionNew.read().format("org.apache.spark.sql.cassandra").options(options).load();
                df.createOrReplaceTempView("temp_cassandra_table");
                filter = filter.replaceAll(path, "temp_cassandra_table");
                sparkSessionNew.sql(filter).show();
            }
        }

        System.exit(0);
    }
}
