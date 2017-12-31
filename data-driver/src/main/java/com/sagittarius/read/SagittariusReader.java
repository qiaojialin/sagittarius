package com.sagittarius.read;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
//import com.datastax.spark.connector.japi.CassandraRow;
//import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.sagittarius.bean.common.HostMetricPair;
import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.bean.common.TypePartitionPair;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.AggregationType;
import com.sagittarius.bean.query.Shift;
import com.sagittarius.bean.result.*;
import com.sagittarius.bean.table.*;
import com.sagittarius.cache.Cache;
import com.sagittarius.exceptions.SparkException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.read.internals.QueryStatement;
import com.sagittarius.util.TimeUtil;
import com.sagittarius.write.SagittariusWriter;
import com.sagittarius.write.Writer;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import scala.Tuple2;
//
//import javax.validation.constraints.NotNull;
//import java.awt.*;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
//import java.time.ZoneOffset;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

//import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
//import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;

public class SagittariusReader implements Reader {
    private Session session;
    private MappingManager mappingManager;
//    private JavaSparkContext sparkContext;
    private Cache<HostMetricPair, TypePartitionPair> cache;
    private static long SECOND_TO_MICROSECOND = 1000000L;
//    private PreparedStatement aggIntStatement;
//    private PreparedStatement aggLongStatement;
//    private PreparedStatement aggFloatStatement;
//    private PreparedStatement aggDoubleStatement;
//    private PreparedStatement aggStringStatement;
//    private PreparedStatement aggBooleanStatement;
//    private PreparedStatement aggGeoStatement;

//    public SagittariusReader(Session session, MappingManager mappingManager, JavaSparkContext sparkContext, Cache<HostMetricPair, TypePartitionPair> cache) {
    public SagittariusReader(Session session, MappingManager mappingManager, Cache<HostMetricPair, TypePartitionPair> cache) {
        this.session = session;
        this.mappingManager = mappingManager;
//        this.sparkContext = sparkContext;
        this.cache = cache;
//        aggIntStatement = session.prepare("select count(*), max(*), min(*), sum(*) from data_int where host = :h and metric = :m and time_slice = :t");
//        aggLongStatement = session.prepare("select count(*), max(*), min(*), sum(*) from data_long where host = :h and metric = :m and time_slice = :t");
//        aggFloatStatement = session.prepare("select count(*), max(*), min(*), sum(*) from data_float where host = :h and metric = :m and time_slice = :t");
//        aggDoubleStatement = session.prepare("select count(*), max(*), min(*), sum(*) from data_double where host = :h and metric = :m and time_slice = :t");
//        aggBooleanStatement = session.prepare("select count(*) from data_boolean where host = :h and metric = :m and time_slice = :t");
//        aggStringStatement = session.prepare("select count(*) from data_text where host = :h and metric = :m and time_slice = :t");
//        aggGeoStatement = session.prepare("select count(*) from data_geo where host = :h and metric = :m and time_slice = :t");
    }

    private Map<String, Map<String, Set<String>>> getTimeSlicePartedHostMetrics(List<HostMetric> hostMetrics, long time) {
        Map<String, Map<String, Set<String>>> timeSliceHostMetric = new HashMap<>();

        for (HostMetric hostMetric : hostMetrics) {
            String timeSlice = TimeUtil.generateTimeSlice(time, hostMetric.getTimePartition());
            if (timeSliceHostMetric.containsKey(timeSlice)) {
                Map<String, Set<String>> setMap = timeSliceHostMetric.get(timeSlice);
                setMap.get("hosts").add(hostMetric.getHost());
                setMap.get("metrics").add(hostMetric.getMetric());
            } else {
                Map<String, Set<String>> setMap = new HashMap<>();
                Set<String> hostSet = new HashSet<>();
                Set<String> metricSet = new HashSet<>();
                hostSet.add(hostMetric.getHost());
                metricSet.add(hostMetric.getMetric());
                setMap.put("hosts", hostSet);
                setMap.put("metrics", metricSet);
                timeSliceHostMetric.put(timeSlice, setMap);
            }
        }

        return timeSliceHostMetric;
    }

    public List<HostMetric> getHostMetrics(List<String> hosts, List<String> metrics) {
        List<HostMetric> result = new ArrayList<>();
        List<String> queryHosts = new ArrayList<>(), queryMetrics = new ArrayList<>();
        //first visit cache, if do not exist in cache, then query cassandra
        for (String host : hosts) {
            for (String metric : metrics) {
                TypePartitionPair typePartition = cache.get(new HostMetricPair(host, metric));
                if (typePartition != null) {
                    result.add(new HostMetric(host, metric, typePartition.getTimePartition(), typePartition.getValueType(), null));
                } else {
                    queryHosts.add(host);
                    queryMetrics.add(metric);
                }
            }
        }
        if(queryHosts.isEmpty() || queryMetrics.isEmpty()){
            return result;
        }
        //query cassandra
        Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
        Statement statement = new SimpleStatement(String.format(QueryStatement.HOST_METRICS_QUERY_STATEMENT, generateInStatement(queryHosts), generateInStatement(queryMetrics)));
        ResultSet rs = session.execute(statement);
        List<HostMetric> hostMetrics = mapper.map(rs).all();
        result.addAll(hostMetrics);
        //update cache
        for (HostMetric hostMetric : hostMetrics) {
            cache.put(new HostMetricPair(hostMetric.getHost(), hostMetric.getMetric()), new TypePartitionPair(hostMetric.getTimePartition(), hostMetric.getValueType()));
        }

        return result;
    }

    private String generateInStatement(Collection<String> params) {
        StringBuilder sb = new StringBuilder();
        for (String param : params) {
            sb.append("'").append(param).append("'").append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    private String getTableByType(ValueType valueType) {
        String table = null;
        switch (valueType) {
            case INT:
                table = "data_int";
                break;
            case LONG:
                table = "data_long";
                break;
            case FLOAT:
                table = "data_float";
                break;
            case DOUBLE:
                table = "data_double";
                break;
            case BOOLEAN:
                table = "data_boolean";
                break;
            case STRING:
                table = "data_text";
                break;
            case GEO:
                table = "data_geo";
                break;
        }
        return table;
    }

    private List<ResultSet> getPointResultSet(List<String> hosts, List<String> metrics, long time, ValueType valueType) {
        String table = getTableByType(valueType);
        List<ResultSet> resultSets = new ArrayList<>();
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);

        Map<String, Map<String, Set<String>>> timeSliceHostMetric = getTimeSlicePartedHostMetrics(hostMetrics, time);
        for (Map.Entry<String, Map<String, Set<String>>> entry : timeSliceHostMetric.entrySet()) {
            SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.POINT_QUERY_STATEMENT, table, generateInStatement(entry.getValue().get("hosts")), generateInStatement(entry.getValue().get("metrics")), entry.getKey(), time));
            ResultSet set = session.execute(statement);
            resultSets.add(set);
        }

        return resultSets;
    }

    @Override
    public Map<ValueType, Map<String, Set<String>>> getValueType(List<String> hosts, List<String> metrics) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<ValueType, Map<String, Set<String>>> result = new HashMap<>();

        try {
            List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
            for (HostMetric hostMetric : hostMetrics) {
                ValueType type = hostMetric.getValueType();
                if (result.containsKey(type)) {
                    Map<String, Set<String>> setMap = result.get(type);
                    setMap.get("hosts").add(hostMetric.getHost());
                    setMap.get("metrics").add(hostMetric.getMetric());
                } else {
                    Map<String, Set<String>> setMap = new HashMap<>();
                    Set<String> hostSet = new HashSet<>();
                    Set<String> metricSet = new HashSet<>();
                    hostSet.add(hostMetric.getHost());
                    metricSet.add(hostMetric.getMetric());
                    setMap.put("hosts", hostSet);
                    setMap.put("metrics", metricSet);
                    result.put(type, setMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public ValueType getValueType(String host, String metric) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            List<String> hosts = new ArrayList<>(), metrics = new ArrayList<>();
            hosts.add(host);
            metrics.add(metric);
            List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
            if (hostMetrics.size() == 0) {
                return null;
            } else {
                return hostMetrics.get(0).getValueType();
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Map<String, Map<String, IntPoint>> getIntPoint(List<String> hosts, List<String> metrics, long time) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, IntPoint>> result = new HashMap<>();

        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.INT);
            List<IntData> datas = new ArrayList<>();
            Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
            for (ResultSet rs : resultSets) {
                datas.addAll(mapper.map(rs).all());
            }

            for (IntData data : datas) {
                String host = data.getHost();
                String metric = data.getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new IntPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, IntPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new IntPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, LongPoint>> getLongPoint(List<String> hosts, List<String> metrics, long time) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, LongPoint>> result = new HashMap<>();

        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.LONG);
            List<LongData> datas = new ArrayList<>();
            Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
            for (ResultSet rs : resultSets) {
                datas.addAll(mapper.map(rs).all());
            }

            for (LongData data : datas) {
                String host = data.getHost();
                String metric = data.getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new LongPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, LongPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new LongPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, FloatPoint>> getFloatPoint(List<String> hosts, List<String> metrics, long time) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, FloatPoint>> result = new HashMap<>();

        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.FLOAT);
            List<FloatData> datas = new ArrayList<>();
            Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
            for (ResultSet rs : resultSets) {
                datas.addAll(mapper.map(rs).all());
            }

            for (FloatData data : datas) {
                String host = data.getHost();
                String metric = data.getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new FloatPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, FloatPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new FloatPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, DoublePoint>> getDoublePoint(List<String> hosts, List<String> metrics, long time) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, DoublePoint>> result = new HashMap<>();

        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.DOUBLE);
            List<DoubleData> datas = new ArrayList<>();
            Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
            for (ResultSet rs : resultSets) {
                datas.addAll(mapper.map(rs).all());
            }

            for (DoubleData data : datas) {
                String host = data.getHost();
                String metric = data.getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new DoublePoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, DoublePoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new DoublePoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, BooleanPoint>> getBooleanPoint(List<String> hosts, List<String> metrics, long time) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, BooleanPoint>> result = new HashMap<>();

        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.BOOLEAN);
            List<BooleanData> datas = new ArrayList<>();
            Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
            for (ResultSet rs : resultSets) {
                datas.addAll(mapper.map(rs).all());
            }

            for (BooleanData data : datas) {
                String host = data.getHost();
                String metric = data.getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new BooleanPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, BooleanPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new BooleanPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, StringPoint>> getStringPoint(List<String> hosts, List<String> metrics, long time) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, StringPoint>> result = new HashMap<>();

        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.STRING);
            List<StringData> datas = new ArrayList<>();
            Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
            for (ResultSet rs : resultSets) {
                datas.addAll(mapper.map(rs).all());
            }

            for (StringData data : datas) {
                String host = data.getHost();
                String metric = data.getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new StringPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, StringPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new StringPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, GeoPoint>> getGeoPoint(List<String> hosts, List<String> metrics, long time) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, GeoPoint>> result = new HashMap<>();

        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.GEO);
            List<GeoData> datas = new ArrayList<>();
            Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
            for (ResultSet rs : resultSets) {
                datas.addAll(mapper.map(rs).all());
            }

            for (GeoData data : datas) {
                String host = data.getHost();
                String metric = data.getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new GeoPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                } else {
                    Map<String, GeoPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new GeoPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    private List<ResultSet> getPointResultSet(String host, String metric, long time, ValueType valueType, Shift shift) {
        String table = getTableByType(valueType);
        List<ResultSet> resultSets = new ArrayList<>();

        List<String> hosts = new ArrayList<>(), metrics = new ArrayList<>();
        hosts.add(host);
        metrics.add(metric);
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
        if (hostMetrics.size() == 0) {
            return resultSets;
        }

        Statement statement;
        String timeSlice = TimeUtil.generateTimeSlice(time, hostMetrics.get(0).getTimePartition());

        if (shift == Shift.NEAREST) {
            statement = new SimpleStatement(String.format(QueryStatement.POINT_BEFORE_SHIFT_QUERY_STATEMENT, table, host, metric, timeSlice, time));
            ResultSet setBefore = session.execute(statement);
            if (!setBefore.isExhausted())
                resultSets.add(setBefore);
            statement = new SimpleStatement(String.format(QueryStatement.POINT_AFTER_SHIFT_QUERY_STATEMENT, table, host, metric, timeSlice, time));
            ResultSet setAfter = session.execute(statement);
            if (!setAfter.isExhausted())
                resultSets.add(setAfter);
            return resultSets;
        }

        String queryStatement = null;
        switch (shift) {
            case BEFORE:
                queryStatement = QueryStatement.POINT_BEFORE_SHIFT_QUERY_STATEMENT;
                break;
            case AFTER:
                queryStatement = QueryStatement.POINT_AFTER_SHIFT_QUERY_STATEMENT;
                break;
        }

        statement = new SimpleStatement(String.format(queryStatement, table, host, metric, timeSlice, time));
        ResultSet set = session.execute(statement);
        if (!set.isExhausted())
            resultSets.add(set);

        return resultSets;
    }

    @Override
    public IntPoint getFuzzyIntPoint(String hosts, String metrics, long time, Shift shift) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.INT, shift);
            Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
            if (resultSets.size() == 0) {
                return null;
            } else if (resultSets.size() == 1) {
                IntData data = mapper.map(resultSets.get(0)).one();
                return new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
            } else {
                IntData dataBefore = mapper.map(resultSets.get(0)).one();
                IntData dataAfter = mapper.map(resultSets.get(1)).one();
                if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                    return new IntPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
                else
                    return new IntPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public LongPoint getFuzzyLongPoint(String hosts, String metrics, long time, Shift shift) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.LONG, shift);
            Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
            if (resultSets.size() == 0) {
                return null;
            } else if (resultSets.size() == 1) {
                LongData data = mapper.map(resultSets.get(0)).one();
                return new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
            } else {
                LongData dataBefore = mapper.map(resultSets.get(0)).one();
                LongData dataAfter = mapper.map(resultSets.get(1)).one();
                if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                    return new LongPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
                else
                    return new LongPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public FloatPoint getFuzzyFloatPoint(String hosts, String metrics, long time, Shift shift) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.FLOAT, shift);
            Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
            if (resultSets.size() == 0) {
                return null;
            } else if (resultSets.size() == 1) {
                FloatData data = mapper.map(resultSets.get(0)).one();
                return new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
            } else {
                FloatData dataBefore = mapper.map(resultSets.get(0)).one();
                FloatData dataAfter = mapper.map(resultSets.get(1)).one();
                if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                    return new FloatPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
                else
                    return new FloatPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public DoublePoint getFuzzyDoublePoint(String hosts, String metrics, long time, Shift shift) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.DOUBLE, shift);
            Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
            if (resultSets.size() == 0) {
                return null;
            } else if (resultSets.size() == 1) {
                DoubleData data = mapper.map(resultSets.get(0)).one();
                return new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
            } else {
                DoubleData dataBefore = mapper.map(resultSets.get(0)).one();
                DoubleData dataAfter = mapper.map(resultSets.get(1)).one();
                if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                    return new DoublePoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
                else
                    return new DoublePoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public BooleanPoint getFuzzyBooleanPoint(String hosts, String metrics, long time, Shift shift) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.BOOLEAN, shift);
            Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
            if (resultSets.size() == 0) {
                return null;
            } else if (resultSets.size() == 1) {
                BooleanData data = mapper.map(resultSets.get(0)).one();
                return new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
            } else {
                BooleanData dataBefore = mapper.map(resultSets.get(0)).one();
                BooleanData dataAfter = mapper.map(resultSets.get(1)).one();
                if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                    return new BooleanPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
                else
                    return new BooleanPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public StringPoint getFuzzyStringPoint(String hosts, String metrics, long time, Shift shift) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.STRING, shift);
            Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
            if (resultSets.size() == 0) {
                return null;
            } else if (resultSets.size() == 1) {
                StringData data = mapper.map(resultSets.get(0)).one();
                return new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue());
            } else {
                StringData dataBefore = mapper.map(resultSets.get(0)).one();
                StringData dataAfter = mapper.map(resultSets.get(1)).one();
                if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                    return new StringPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getValue());
                else
                    return new StringPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getValue());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public GeoPoint getFuzzyGeoPoint(String hosts, String metrics, long time, Shift shift) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            List<ResultSet> resultSets = getPointResultSet(hosts, metrics, time, ValueType.GEO, shift);
            Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
            if (resultSets.size() == 0) {
                return null;
            } else if (resultSets.size() == 1) {
                GeoData data = mapper.map(resultSets.get(0)).one();
                return new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude());
            } else {
                GeoData dataBefore = mapper.map(resultSets.get(0)).one();
                GeoData dataAfter = mapper.map(resultSets.get(1)).one();
                if (time - dataBefore.getPrimaryTime() >= dataAfter.getPrimaryTime() - time)
                    return new GeoPoint(dataAfter.getMetric(), dataAfter.getPrimaryTime(), dataAfter.secondaryTimeUnboxed(), dataAfter.getLatitude(), dataAfter.getLongitude());
                else
                    return new GeoPoint(dataBefore.getMetric(), dataBefore.getPrimaryTime(), dataBefore.secondaryTimeUnboxed(), dataBefore.getLatitude(), dataBefore.getLongitude());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    private Result<Latest> getLatestResult(List<String> hosts, List<String> metrics) {
        String table = "latest";
        SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.LATEST_TIMESLICE_QUERY_STATEMENT, table, generateInStatement(hosts), generateInStatement(metrics)));
        ResultSet rs = session.execute(statement);
        Mapper<Latest> mapper = mappingManager.mapper(Latest.class);
        return mapper.map(rs);
    }

    private ResultSet getPointResultSet(String host, String metric, String timeSlice, ValueType valueType) {
        String table = getTableByType(valueType);
        SimpleStatement statement = new SimpleStatement(String.format(QueryStatement.LATEST_POINT_QUERY_STATEMENT, table, host, metric, timeSlice));
        ResultSet set = session.execute(statement);
        return set;
    }

    @Override
    public Map<String, Map<String, IntPoint>> getIntLatest(List<String> hosts, List<String> metrics) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, IntPoint>> result = new HashMap<>();

        try {
            Result<Latest> latests = getLatestResult(hosts, metrics);
            Mapper<IntData> mapperInt = mappingManager.mapper(IntData.class);

            for (Latest latest : latests) {
                String host = latest.getHost();
                String metric = latest.getMetric();
                String timeSlice = latest.getTimeSlice();
                ResultSet rs = getPointResultSet(host, metric, timeSlice, ValueType.INT);
                //if data type mismatch, then the rs contains nothing.
                List<IntData> r = mapperInt.map(rs).all();
                if(r.isEmpty()){
                    continue;
                }
                IntData data = r.get(0);
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new IntPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, IntPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new IntPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, LongPoint>> getLongLatest(List<String> hosts, List<String> metrics) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, LongPoint>> result = new HashMap<>();

        try {
            Result<Latest> latests = getLatestResult(hosts, metrics);
            Mapper<LongData> mapperLong = mappingManager.mapper(LongData.class);

            for (Latest latest : latests) {
                String host = latest.getHost();
                String metric = latest.getMetric();
                String timeSlice = latest.getTimeSlice();
                ResultSet rs = getPointResultSet(host, metric, timeSlice, ValueType.LONG);
                //if data type mismatch, then the rs contains nothing.
                List<LongData> r = mapperLong.map(rs).all();
                if(r.isEmpty()){
                    continue;
                }
                LongData data = r.get(0);
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new LongPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, LongPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new LongPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, FloatPoint>> getFloatLatest(List<String> hosts, List<String> metrics) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, FloatPoint>> result = new HashMap<>();

        try {
            Result<Latest> latests = getLatestResult(hosts, metrics);
            Mapper<FloatData> mapperFloat = mappingManager.mapper(FloatData.class);

            for (Latest latest : latests) {
                String host = latest.getHost();
                String metric = latest.getMetric();
                String timeSlice = latest.getTimeSlice();
                ResultSet rs = getPointResultSet(host, metric, timeSlice, ValueType.FLOAT);
                //if data type mismatch, then the rs contains nothing.
                List<FloatData> r = mapperFloat.map(rs).all();
                if(r.isEmpty()){
                    continue;
                }
                FloatData data = r.get(0);
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new FloatPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, FloatPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new FloatPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, DoublePoint>> getDoubleLatest(List<String> hosts, List<String> metrics) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, DoublePoint>> result = new HashMap<>();

        try {
            Result<Latest> latests = getLatestResult(hosts, metrics);
            Mapper<DoubleData> mapperDouble = mappingManager.mapper(DoubleData.class);

            for (Latest latest : latests) {
                String host = latest.getHost();
                String metric = latest.getMetric();
                String timeSlice = latest.getTimeSlice();
                ResultSet rs = getPointResultSet(host, metric, timeSlice, ValueType.DOUBLE);
                //if data type mismatch, then the rs contains nothing.
                List<DoubleData> r = mapperDouble.map(rs).all();
                if(r.isEmpty()){
                    continue;
                }
                DoubleData data = r.get(0);
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new DoublePoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, DoublePoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new DoublePoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, BooleanPoint>> getBooleanLatest(List<String> hosts, List<String> metrics) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, BooleanPoint>> result = new HashMap<>();

        try {
            Result<Latest> latests = getLatestResult(hosts, metrics);
            Mapper<BooleanData> mapperBoolean = mappingManager.mapper(BooleanData.class);

            for (Latest latest : latests) {
                String host = latest.getHost();
                String metric = latest.getMetric();
                String timeSlice = latest.getTimeSlice();
                ResultSet rs = getPointResultSet(host, metric, timeSlice, ValueType.BOOLEAN);
                //if data type mismatch, then the rs contains nothing.
                List<BooleanData> r = mapperBoolean.map(rs).all();
                if(r.isEmpty()){
                    continue;
                }
                BooleanData data = r.get(0);
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new BooleanPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, BooleanPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new BooleanPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, StringPoint>> getStringLatest(List<String> hosts, List<String> metrics) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, StringPoint>> result = new HashMap<>();

        try {
            Result<Latest> latests = getLatestResult(hosts, metrics);
            Mapper<StringData> mapperString = mappingManager.mapper(StringData.class);

            for (Latest latest : latests) {
                String host = latest.getHost();
                String metric = latest.getMetric();
                String timeSlice = latest.getTimeSlice();
                ResultSet rs = getPointResultSet(host, metric, timeSlice, ValueType.STRING);
                //if data type mismatch, then the rs contains nothing.
                List<StringData> r = mapperString.map(rs).all();
                if(r.isEmpty()){
                    continue;
                }
                StringData data = r.get(0);
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new StringPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    Map<String, StringPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new StringPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, GeoPoint>> getGeoLatest(List<String> hosts, List<String> metrics) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        Map<String, Map<String, GeoPoint>> result = new HashMap<>();

        try {
            Result<Latest> latests = getLatestResult(hosts, metrics);
            Mapper<GeoData> mapperGeo = mappingManager.mapper(GeoData.class);

            for (Latest latest : latests) {
                String host = latest.getHost();
                String metric = latest.getMetric();
                String timeSlice = latest.getTimeSlice();
                ResultSet rs = getPointResultSet(host, metric, timeSlice, ValueType.GEO);
                //if data type mismatch, then the rs contains nothing.
                List<GeoData> r = mapperGeo.map(rs).all();
                if(r.isEmpty()){
                    continue;
                }
                GeoData data = r.get(0);
                if (result.containsKey(host)) {
                    result.get(host).put(metric, new GeoPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                } else {
                    Map<String, GeoPoint> metricMap = new HashMap<>();
                    metricMap.put(metric, new GeoPoint(metric, data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                    result.put(host, metricMap);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    private List<String> getRangeQueryString(List<String> hosts, List<String> metrics, long startTime, long endTime, ValueType valueType, boolean desc) {
        String table = getTableByType(valueType);
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
        Map<String, Map<String, Set<String>>> timeSliceHostMetric = getTimeSlicePartedHostMetrics(hostMetrics, startTime);
        List<String> querys = new ArrayList<>();
        long startTimeSecond = startTime / SECOND_TO_MICROSECOND;
        long endTimeSecond = endTime / SECOND_TO_MICROSECOND;

        for (Map.Entry<String, Map<String, Set<String>>> entry : timeSliceHostMetric.entrySet()) {
            String startTimeSlice = entry.getKey();
            String hostsString = generateInStatement(entry.getValue().get("hosts"));
            String metricsString = generateInStatement(entry.getValue().get("metrics"));
            TimePartition timePartition;
            if (startTimeSlice.contains("D")) {
                timePartition = TimePartition.DAY;
            } else if (startTimeSlice.contains("W")) {
                timePartition = TimePartition.WEEK;
            } else if (startTimeSlice.contains("M")) {
                timePartition = TimePartition.MONTH;
            } else {
                timePartition = TimePartition.YEAR;
            }

            String endTimeSlice = TimeUtil.generateTimeSlice(endTime, timePartition);
            if (startTimeSlice.equals(endTimeSlice)) {
                if(desc){
                    String query = String.format(QueryStatement.IN_PARTITION_QUERY_STATEMENT_DESC, table, hostsString, metricsString, startTimeSlice, startTime, endTime);
                    querys.add(query);
                }
                else {
                    String query = String.format(QueryStatement.IN_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, startTimeSlice, startTime, endTime);
                    querys.add(query);
                }
                continue;
            }

            LocalDateTime start = LocalDateTime.ofEpochSecond(startTimeSecond, 0, TimeUtil.zoneOffset);
            LocalDateTime end = LocalDateTime.ofEpochSecond(endTimeSecond, 0, TimeUtil.zoneOffset);
            List<LocalDateTime> totalDates = new ArrayList<>();
            while (!start.isAfter(end)) {
                totalDates.add(start);
                switch (timePartition) {
                    case DAY:
                        start = start.plusDays(1);
                        break;
                    case WEEK:
                        start = start.plusWeeks(1);
                        break;
                    case MONTH:
                        start = start.plusMonths(1);
                        break;
                    case YEAR:
                        start = start.plusYears(1);
                        break;
                }
            }
            if(desc){
                String startQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT_DESC, table, hostsString, metricsString, startTimeSlice, ">=", startTime);
                querys.add(startQuery);
            }
            else {
                String startQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, startTimeSlice, ">=", startTime);
                querys.add(startQuery);
            }
            for (int i = 1; i < totalDates.size() - 1; ++i) {
                //the last datetime may be in the same timepartition with the end datetime, so it should be processed separately.
                if(desc){
                    String query = String.format(QueryStatement.WHOLE_PARTITION_QUERY_STATEMENT_DESC, table, hostsString, metricsString, TimeUtil.generateTimeSlice(totalDates.get(i).toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, timePartition));
                    querys.add(query);
                }
                else {
                    String query = String.format(QueryStatement.WHOLE_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, TimeUtil.generateTimeSlice(totalDates.get(i).toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, timePartition));
                    querys.add(query);
                }
            }
            LocalDateTime last = totalDates.get(totalDates.size() - 1);
            long lastMicroSecond = last.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND;
            String lastTimeSlice = TimeUtil.generateTimeSlice(lastMicroSecond, timePartition);
            boolean ifRepeat = lastTimeSlice.equals(endTimeSlice) || lastTimeSlice.equals(startTimeSlice);
            if (!ifRepeat) {
                if(desc){
                    String query = String.format(QueryStatement.WHOLE_PARTITION_QUERY_STATEMENT_DESC, table, hostsString, metricsString, TimeUtil.generateTimeSlice(last.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, timePartition));
                    querys.add(query);
                }
                else {
                    String query = String.format(QueryStatement.WHOLE_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, TimeUtil.generateTimeSlice(last.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, timePartition));
                    querys.add(query);
                }
            }
            if(desc){
                String endQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT_DESC, table, hostsString, metricsString, endTimeSlice, "<=", endTime);
                querys.add(endQuery);
            }
            else {
                String endQuery = String.format(QueryStatement.PARTIAL_PARTITION_QUERY_STATEMENT, table, hostsString, metricsString, endTimeSlice, "<=", endTime);
                querys.add(endQuery);
            }

        }

        return querys;
    }

    @Override
    public Map<String, Map<String, List<IntPoint>>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, List<IntPoint>>> result = new HashMap<>();
        List<IntData> datas = new ArrayList<>();

        try {
            List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.INT, desc);
            Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
            for (String query : querys) {
                ResultSet rs = session.execute(query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (IntData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<IntPoint> points = new ArrayList<>();
                    points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<IntPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<LongPoint>>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, List<LongPoint>>> result = new HashMap<>();
        List<LongData> datas = new ArrayList<>();

        try {
            List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.LONG, desc);
            Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
            for (String query : querys) {
                ResultSet rs = session.execute(query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (LongData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<LongPoint> points = new ArrayList<>();
                    points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<LongPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<FloatPoint>>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;
        Map<String, Map<String, List<FloatPoint>>> result = new HashMap<>();
        List<FloatData> datas = new ArrayList<>();

        try {
            List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.FLOAT, desc);
            Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
            for (String query : querys) {
                ResultSet rs = session.execute(query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (FloatData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<FloatPoint> points = new ArrayList<>();
                    points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<FloatPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<DoublePoint>>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, List<DoublePoint>>> result = new HashMap<>();
        List<DoubleData> datas = new ArrayList<>();

        try {
            List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.DOUBLE, desc);
            Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
            for (String query : querys) {
                ResultSet rs = session.execute(query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (DoubleData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<DoublePoint> points = new ArrayList<>();
                    points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<DoublePoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<BooleanPoint>>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, List<BooleanPoint>>> result = new HashMap<>();
        List<BooleanData> datas = new ArrayList<>();

        try {
            List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.BOOLEAN, desc);
            Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
            for (String query : querys) {
                ResultSet rs = session.execute(query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (BooleanData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<BooleanPoint> points = new ArrayList<>();
                    points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<BooleanPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<StringPoint>>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, List<StringPoint>>> result = new HashMap<>();
        List<StringData> datas = new ArrayList<>();

        try {
            List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.STRING, desc);
            Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
            for (String query : querys) {
                ResultSet rs = session.execute(query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (StringData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<StringPoint> points = new ArrayList<>();
                    points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<StringPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<GeoPoint>>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, List<GeoPoint>>> result = new HashMap<>();
        List<GeoData> datas = new ArrayList<>();

        try {
            List<String> querys = getRangeQueryString(hosts, metrics, startTime, endTime, ValueType.GEO, desc);
            Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
            for (String query : querys) {
                ResultSet rs = session.execute(query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (GeoData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                } else {
                    List<GeoPoint> points = new ArrayList<>();
                    points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                Map<String, List<GeoPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    private List<String> getRangeQueryPredicates(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) {
        //spark driver query metadata
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
        Map<String, Map<String, Set<String>>> timeSliceHostMetric = getTimeSlicePartedHostMetrics(hostMetrics, startTime);

        List<String> predicates = new ArrayList<>();
        long startTimeSecond = startTime / SECOND_TO_MICROSECOND;
        long endTimeSecond = endTime / SECOND_TO_MICROSECOND;

        for (Map.Entry<String, Map<String, Set<String>>> entry : timeSliceHostMetric.entrySet()) {
            String startTimeSlice = entry.getKey();
            String hostsString = generateInStatement(entry.getValue().get("hosts"));
            String metricsString = generateInStatement(entry.getValue().get("metrics"));
            TimePartition timePartition;
            if (startTimeSlice.contains("D")) {
                timePartition = TimePartition.DAY;
            } else if (startTimeSlice.contains("W")) {
                timePartition = TimePartition.WEEK;
            } else if (startTimeSlice.contains("M")) {
                timePartition = TimePartition.MONTH;
            } else {
                timePartition = TimePartition.YEAR;
            }

            String endTimeSlice = TimeUtil.generateTimeSlice(endTime, timePartition);
            if (startTimeSlice.equals(endTimeSlice)) {
                if(desc){
                    String predicate = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_DESC, hostsString, metricsString, startTimeSlice, startTime, endTime, filter);
                    predicates.add(predicate);
                }
                else{
                    String predicate = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT, hostsString, metricsString, startTimeSlice, startTime, endTime, filter);
                    predicates.add(predicate);
                }

                continue;
            }

            LocalDateTime start = LocalDateTime.ofEpochSecond(startTimeSecond, 0, TimeUtil.zoneOffset);
            LocalDateTime end = LocalDateTime.ofEpochSecond(endTimeSecond, 0, TimeUtil.zoneOffset);
            List<LocalDateTime> totalDates = new ArrayList<>();
            while (!start.isAfter(end)) {
                totalDates.add(start);
                switch (timePartition) {
                    case DAY:
                        start = start.plusDays(1);
                        break;
                    case WEEK:
                        start = start.plusWeeks(1);
                        break;
                    case MONTH:
                        start = start.plusMonths(1);
                        break;
                    case YEAR:
                        start = start.plusYears(1);
                        break;
                }
            }
            if(desc){
                String startPredicate = String.format(QueryStatement.PARTIAL_PARTITION_WHERE_STATEMENT_DESC, hostsString, metricsString, startTimeSlice, ">=", startTime, filter);
                predicates.add(startPredicate);
            }
            else {
                String startPredicate = String.format(QueryStatement.PARTIAL_PARTITION_WHERE_STATEMENT, hostsString, metricsString, startTimeSlice, ">=", startTime, filter);
                predicates.add(startPredicate);
            }


            for (int i = 1; i < totalDates.size() - 1; ++i) {
                //the last datetime may be in the same timepartition with the end datetime, so it should be processed separately.
                if(desc){
                    String predicate = String.format(QueryStatement.WHOLE_PARTITION_WHERE_STATEMENT_DESC, hostsString, metricsString, TimeUtil.generateTimeSlice(totalDates.get(i).toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, timePartition), filter);
                    predicates.add(predicate);
                }
                else {
                    String predicate = String.format(QueryStatement.WHOLE_PARTITION_WHERE_STATEMENT, hostsString, metricsString, TimeUtil.generateTimeSlice(totalDates.get(i).toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, timePartition), filter);
                    predicates.add(predicate);
                }
            }
            LocalDateTime last = totalDates.get(totalDates.size() - 1);
            long lastMS = last.toEpochSecond(TimeUtil.zoneOffset)*SECOND_TO_MICROSECOND;
            String lastTimeSlice = TimeUtil.generateTimeSlice(lastMS, timePartition);
            boolean ifRepeat = lastTimeSlice.equals(endTimeSlice) || lastTimeSlice.equals(startTimeSlice);
            if (!ifRepeat) {
                if(desc){
                    String predicate = String.format(QueryStatement.WHOLE_PARTITION_WHERE_STATEMENT_DESC, hostsString, metricsString, TimeUtil.generateTimeSlice(last.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, timePartition), filter);
                    predicates.add(predicate);
                }
                else {
                    String predicate = String.format(QueryStatement.WHOLE_PARTITION_WHERE_STATEMENT, hostsString, metricsString, TimeUtil.generateTimeSlice(last.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, timePartition), filter);
                    predicates.add(predicate);
                }
            }

            if(desc){
                String endPredicate = String.format(QueryStatement.PARTIAL_PARTITION_WHERE_STATEMENT_DESC, hostsString, metricsString, endTimeSlice, "<=", endTime, filter);
                predicates.add(endPredicate);
            }
            else {
                String endPredicate = String.format(QueryStatement.PARTIAL_PARTITION_WHERE_STATEMENT, hostsString, metricsString, endTimeSlice, "<=", endTime, filter);
                predicates.add(endPredicate);
            }
        }

        return predicates;
    }

    @Override
    public Map<String, Map<String, List<IntPoint>>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        String queryFilter = filter == null ? "" : " and " + filter;

        Map<String, Map<String, List<IntPoint>>> result = new HashMap<>();
        List<IntData> datas = new ArrayList<>();

        try {
            List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime, queryFilter, desc);
            if (predicates.size() == 0) {
                return null;
            }
            Mapper<IntData> mapper = mappingManager.mapper(IntData.class);
            for (String query : predicates) {
                ResultSet rs = session.execute("select * from data_int where " + query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (IntData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<IntPoint> points = new ArrayList<>();
                    points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<IntPoint> points = new ArrayList<>();
                points.add(new IntPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<IntPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<LongPoint>>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        String queryFilter = filter == null ? "" : " and " + filter;

        Map<String, Map<String, List<LongPoint>>> result = new HashMap<>();
        List<LongData> datas = new ArrayList<>();

        try {
            List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime, queryFilter, desc);
            if (predicates.size() == 0) {
                return null;
            }
            Mapper<LongData> mapper = mappingManager.mapper(LongData.class);
            for (String query : predicates) {
                ResultSet rs = session.execute("select * from data_long where " + query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (LongData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<LongPoint> points = new ArrayList<>();
                    points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<LongPoint> points = new ArrayList<>();
                points.add(new LongPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<LongPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<FloatPoint>>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        String queryFilter = filter == null ? "" : " and " + filter;

        Map<String, Map<String, List<FloatPoint>>> result = new HashMap<>();
        List<FloatData> datas = new ArrayList<>();

        try {
            List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime, queryFilter, desc);
            if (predicates.size() == 0) {
                return null;
            }
            Mapper<FloatData> mapper = mappingManager.mapper(FloatData.class);
            for (String query : predicates) {
                ResultSet rs = session.execute("select * from data_float where " + query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (FloatData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<FloatPoint> points = new ArrayList<>();
                    points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<FloatPoint> points = new ArrayList<>();
                points.add(new FloatPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<FloatPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<DoublePoint>>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        String queryFilter = filter == null ? "" : " and " + filter;

        Map<String, Map<String, List<DoublePoint>>> result = new HashMap<>();
        List<DoubleData> datas = new ArrayList<>();

        try {
            List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime, queryFilter, desc);
            if (predicates.size() == 0) {
                return null;
            }
            Mapper<DoubleData> mapper = mappingManager.mapper(DoubleData.class);
            for (String query : predicates) {
                ResultSet rs = session.execute("select * from data_double where " + query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (DoubleData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<DoublePoint> points = new ArrayList<>();
                    points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<DoublePoint> points = new ArrayList<>();
                points.add(new DoublePoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<DoublePoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<BooleanPoint>>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        String queryFilter = filter == null ? "" : " and " + filter;

        Map<String, Map<String, List<BooleanPoint>>> result = new HashMap<>();
        List<BooleanData> datas = new ArrayList<>();

        try {
            List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime, queryFilter, desc);
            if (predicates.size() == 0) {
                return null;
            }
            Mapper<BooleanData> mapper = mappingManager.mapper(BooleanData.class);
            for (String query : predicates) {
                ResultSet rs = session.execute("select * from data_boolean where " + query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (BooleanData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<BooleanPoint> points = new ArrayList<>();
                    points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<BooleanPoint> points = new ArrayList<>();
                points.add(new BooleanPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<BooleanPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<StringPoint>>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        String queryFilter = filter == null ? "" : " and " + filter;

        Map<String, Map<String, List<StringPoint>>> result = new HashMap<>();
        List<StringData> datas = new ArrayList<>();

        try {
            List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime, queryFilter, desc);
            if (predicates.size() == 0) {
                return null;
            }
            Mapper<StringData> mapper = mappingManager.mapper(StringData.class);
            for (String query : predicates) {
                ResultSet rs = session.execute("select * from data_text where " + query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (StringData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                } else {
                    List<StringPoint> points = new ArrayList<>();
                    points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<StringPoint> points = new ArrayList<>();
                points.add(new StringPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getValue()));
                Map<String, List<StringPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, List<GeoPoint>>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        String queryFilter = filter == null ? "" : " and " + filter;

        Map<String, Map<String, List<GeoPoint>>> result = new HashMap<>();
        List<GeoData> datas = new ArrayList<>();

        try {
            List<String> predicates = getRangeQueryPredicates(hosts, metrics, startTime, endTime, queryFilter, desc);
            if (predicates.size() == 0) {
                return null;
            }
            Mapper<GeoData> mapper = mappingManager.mapper(GeoData.class);
            for (String query : predicates) {
                ResultSet rs = session.execute("select * from data_geo where " + query);
                datas.addAll(mapper.map(rs).all());
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        for (GeoData data : datas) {
            String host = data.getHost();
            String metric = data.getMetric();
            if (result.containsKey(host)) {
                if (result.get(host).containsKey(metric)) {
                    result.get(host).get(metric).add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                } else {
                    List<GeoPoint> points = new ArrayList<>();
                    points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                    result.get(host).put(metric, points);
                }
            } else {
                List<GeoPoint> points = new ArrayList<>();
                points.add(new GeoPoint(data.getMetric(), data.getPrimaryTime(), data.secondaryTimeUnboxed(), data.getLatitude(), data.getLongitude()));
                Map<String, List<GeoPoint>> metricMap = new HashMap<>();
                metricMap.put(metric, points);
                result.put(host, metricMap);
            }
        }

        return result.size() != 0 ? result : null;
    }

    private List<String> getTimeSlices(long startTime, long endTime, TimePartition timePartition){
        ArrayList<String> timeSlices = new ArrayList<>();

        LocalDateTime start = LocalDateTime.ofEpochSecond(startTime/SECOND_TO_MICROSECOND, 0, TimeUtil.zoneOffset);
        LocalDateTime end = LocalDateTime.ofEpochSecond(endTime/SECOND_TO_MICROSECOND, 0, TimeUtil.zoneOffset);
        switch (timePartition){
            case DAY:{
                while (start.isBefore(end)){
                    timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, TimePartition.DAY));
                    start = start.plusDays(1);
                }
                timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, TimePartition.DAY));
                break;
            }
            case WEEK:{
                while (start.isBefore(end)){
                    timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, TimePartition.WEEK));
                    start = start.plusWeeks(1);
                }
                timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, TimePartition.WEEK));
                break;
            }
            case MONTH:{
                while (start.isBefore(end)){
                    timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, TimePartition.MONTH));
                    start= start.plusMonths(1);
                }
                timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, TimePartition.MONTH));
                break;
            }
            case YEAR:{
                while (start.isBefore(end)){
                    timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, TimePartition.YEAR));
                    start = start.plusYears(1);
                }
                timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * SECOND_TO_MICROSECOND, TimePartition.YEAR));
                break;
            }
        }
        return timeSlices;
    }

    @Override
    public Map<String, Map<String, Double>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, Double>> result = new HashMap<>();
        Map<HostMetric, Double> datas = new HashMap<>();

        String queryFilter = filter == null ? "" : " and " + filter + " allow filtering";

        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);

        try {
            switch (aggregationType){
                case MAX:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select max(value) from data_int where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getInt(0));
                        }
                    }
                    break;
                }
                case MIN:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select min(value) from data_int where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getInt(0));
                        }
                    }
                    break;
                }
                case COUNT:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select count(value) from data_int where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getLong(0));
                        }
                    }
                    break;
                }
                case SUM:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select sum(value) from data_int where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getInt(0));
                        }
                    }
                    break;
                }
                case AVG:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select sum(value) from data_int where " + query);
                        SimpleStatement statement2 = new SimpleStatement("select count(value) from data_int where " + query);
                        ResultSet resultSet = session.execute(statement);
                        ResultSet resultSet2 = session.execute(statement2);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getInt(0)/resultSet2.one().getLong(0));
                        }
                    }
                    break;
                }
            }

            for (Map.Entry<HostMetric, Double> data : datas.entrySet()) {
                String host = data.getKey().getHost();
                String metric = data.getKey().getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, data.getValue());
                } else {
                    Map<String, Double> map = new HashMap<>();
                    map.put(metric, data.getValue());
                    result.put(host, map);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, Double>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, Double>> result = new HashMap<>();
        Map<HostMetric, Double> datas = new HashMap<>();

        String queryFilter = filter == null ? "" : " and " + filter + " allow filtering";

        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);

        try {
            switch (aggregationType){
                case MAX:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select max(value) from data_long where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getLong(0));
                        }
                    }
                    break;
                }
                case MIN:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select min(value) from data_long where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getLong(0));
                        }
                    }
                    break;
                }
                case COUNT:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select count(value) from data_long where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getLong(0));
                        }
                    }
                    break;
                }
                case SUM:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select sum(value) from data_long where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getLong(0));
                        }
                    }
                    break;
                }
                case AVG:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select sum(value) from data_long where " + query);
                        SimpleStatement statement2 = new SimpleStatement("select count(value) from data_long where " + query);
                        ResultSet resultSet = session.execute(statement);
                        ResultSet resultSet2 = session.execute(statement2);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getLong(0)/resultSet2.one().getLong(0));
                        }
                    }
                    break;
                }
            }

            for (Map.Entry<HostMetric, Double> data : datas.entrySet()) {
                String host = data.getKey().getHost();
                String metric = data.getKey().getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, data.getValue());
                } else {
                    Map<String, Double> map = new HashMap<>();
                    map.put(metric, data.getValue());
                    result.put(host, map);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, Double>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, Double>> result = new HashMap<>();
        Map<HostMetric, Double> datas = new HashMap<>();

        String queryFilter = filter == null ? "" : " and " + filter + " allow filtering";
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);

        try {
            switch (aggregationType){
                case MAX:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select max(value) from data_float where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getFloat(0));
                        }
                    }
                    break;
                }
                case MIN:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select min(value) from data_float where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getFloat(0));
                        }
                    }
                    break;
                }
                case COUNT:{

                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select count(value) from data_float where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getLong(0));
                        }
                    }
                    break;
                }
                case SUM:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select sum(value) from data_float where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getFloat(0));
                        }
                    }
                    break;
                }
                case AVG:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select sum(value) from data_float where " + query);
                        SimpleStatement statement2 = new SimpleStatement("select count(value) from data_float where " + query);
                        ResultSet resultSet = session.execute(statement);
                        ResultSet resultSet2 = session.execute(statement2);
                        if(resultSet != null){
                            datas.put(hostMetric, (double)resultSet.one().getFloat(0)/resultSet2.one().getLong(0));
                        }
                    }
                    break;
                }
            }

            for (Map.Entry<HostMetric, Double> data : datas.entrySet()) {
                String host = data.getKey().getHost();
                String metric = data.getKey().getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, data.getValue());
                } else {
                    Map<String, Double> map = new HashMap<>();
                    map.put(metric, data.getValue());
                    result.put(host, map);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }


        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, Double>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, Double>> result = new HashMap<>();
        Map<HostMetric, Double> datas = new HashMap<>();

        String queryFilter = filter == null ? "" : " and " + filter + " allow filtering";

        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);

        try {
            switch (aggregationType){
                case MAX:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select max(value) from data_double where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null && !result.isEmpty()){
                            datas.put(hostMetric, resultSet.one().getDouble(0));
                        }
                    }
                    break;
                }
                case MIN:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select min(value) from data_double where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null && !result.isEmpty()){
                            datas.put(hostMetric, resultSet.one().getDouble(0));
                        }
                    }
                    break;
                }
                case COUNT:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select count(value) from data_double where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null && !result.isEmpty()){
                            datas.put(hostMetric, (double)resultSet.one().getLong(0));
                        }
                    }
                    break;
                }
                case SUM:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select sum(value) from data_double where " + query);
                        ResultSet resultSet = session.execute(statement);
                        if(resultSet != null && !result.isEmpty()){
                            datas.put(hostMetric, resultSet.one().getDouble(0));
                        }
                    }
                    break;
                }
                case AVG:{
                    for(HostMetric hostMetric : hostMetrics){
                        String host = hostMetric.getHost();
                        String metric = hostMetric.getMetric();
                        TimePartition timePartition = hostMetric.getTimePartition();
                        List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                        String inTimeSlices = generateInStatement(timeSlices);
                        String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                        SimpleStatement statement = new SimpleStatement("select sum(value) from data_double where " + query);
                        SimpleStatement statement2 = new SimpleStatement("select count(value) from data_double where " + query);
                        ResultSet resultSet = session.execute(statement);
                        ResultSet resultSet2 = session.execute(statement2);
                        if(resultSet != null && !result.isEmpty()){
                            datas.put(hostMetric, resultSet.one().getDouble(0)/resultSet2.one().getLong(0));
                        }
                    }
                    break;
                }
            }

            for (Map.Entry<HostMetric, Double> data : datas.entrySet()) {
                String host = data.getKey().getHost();
                String metric = data.getKey().getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, data.getValue());
                } else {
                    Map<String, Double> map = new HashMap<>();
                    map.put(metric, data.getValue());
                    result.put(host, map);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, Double>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, Double>> result = new HashMap<>();
        Map<HostMetric, Double> datas = new HashMap<>();

        String queryFilter = filter == null ? "" : " and " + filter + " allow filtering";

        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);

        try {
            for(HostMetric hostMetric : hostMetrics){
                String host = hostMetric.getHost();
                String metric = hostMetric.getMetric();
                TimePartition timePartition = hostMetric.getTimePartition();
                List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                String inTimeSlices = generateInStatement(timeSlices);
                String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                SimpleStatement statement = new SimpleStatement("select count(value) from data_boolean where " + query);
                ResultSet resultSet = session.execute(statement);
                if(resultSet != null && !result.isEmpty()){
                    datas.put(hostMetric, (double)resultSet.one().getLong(0));
                }
            }

            for (Map.Entry<HostMetric, Double> data : datas.entrySet()) {
                String host = data.getKey().getHost();
                String metric = data.getKey().getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, data.getValue());
                } else {
                    Map<String, Double> map = new HashMap<>();
                    map.put(metric, data.getValue());
                    result.put(host, map);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, Double>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, Double>> result = new HashMap<>();
        Map<HostMetric, Double> datas = new HashMap<>();

        String queryFilter = filter == null ? "" : " and " + filter + " allow filtering";

        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);

        try {
            for(HostMetric hostMetric : hostMetrics){
                String host = hostMetric.getHost();
                String metric = hostMetric.getMetric();
                TimePartition timePartition = hostMetric.getTimePartition();
                List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                String inTimeSlices = generateInStatement(timeSlices);
                String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                SimpleStatement statement = new SimpleStatement("select count(value) from data_text where " + query);
                ResultSet resultSet = session.execute(statement);
                if(resultSet != null && !result.isEmpty()){
                    datas.put(hostMetric, (double)resultSet.one().getLong(0));
                }
            }

            for (Map.Entry<HostMetric, Double> data : datas.entrySet()) {
                String host = data.getKey().getHost();
                String metric = data.getKey().getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, data.getValue());
                } else {
                    Map<String, Double> map = new HashMap<>();
                    map.put(metric, data.getValue());
                    result.put(host, map);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

    @Override
    public Map<String, Map<String, Double>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        if (endTime < startTime)
            return null;

        Map<String, Map<String, Double>> result = new HashMap<>();
        Map<HostMetric, Double> datas = new HashMap<>();

        String queryFilter = filter == null ? "" : " and " + filter + " allow filtering";

        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);

        try {
            for(HostMetric hostMetric : hostMetrics){
                String host = hostMetric.getHost();
                String metric = hostMetric.getMetric();
                TimePartition timePartition = hostMetric.getTimePartition();
                List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
                String inTimeSlices = generateInStatement(timeSlices);
                String query = String.format(QueryStatement.IN_PARTITION_WHERE_STATEMENT_AGG, host, metric, inTimeSlices, startTime, endTime, queryFilter);
                SimpleStatement statement = new SimpleStatement("select count(value) from data_geo where " + query);
                ResultSet resultSet = session.execute(statement);
                if(resultSet != null && !result.isEmpty()){
                    datas.put(hostMetric, (double)resultSet.one().getLong(0));
                }
            }

            for (Map.Entry<HostMetric, Double> data : datas.entrySet()) {
                String host = data.getKey().getHost();
                String metric = data.getKey().getMetric();
                if (result.containsKey(host)) {
                    result.get(host).put(metric, data.getValue());
                } else {
                    Map<String, Double> map = new HashMap<>();
                    map.put(metric, data.getValue());
                    result.put(host, map);
                }
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result.size() != 0 ? result : null;
    }

//    @Override
//    public void exportIntCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
//        if (endTime < startTime) return;
//
//        //generate time ranges
//        List<Long> timePoints = new ArrayList<>();
//        timePoints.add(startTime);
//        long splitMillis = splitHours * 60 * 60 * 1000;
//        while (startTime + splitMillis < endTime) {
//            startTime = startTime + splitMillis;
//            timePoints.add(startTime);
//        }
//        timePoints.add(endTime);
//
//        String lineSeparator = System.getProperty("line.separator");
//        FileWriter fw = new FileWriter(filePath, true);
//        //construct file header and write to file
//        StringBuilder head = new StringBuilder();
//        head.append("ID号,").append("生成时间,").append("接收时间,");
//        for (String metric : metrics) {
//            head.append(metric).append(",");
//        }
//        head.delete(head.length() - 1, head.length());
//        head.append(lineSeparator);
//        fw.write(head.toString());
//        fw.flush();
//
//        String queryFilter = (filter == null) ? "" : " and " + filter;
//        //construct data output and write to file
//        for (String host : hosts) {
//            List<String> single = new ArrayList<>();
//            single.add(host);
//            for (int j = 0; j < timePoints.size() - 1; ++j) {
//                List<Map.Entry<Long, List<CassandraRow>>> entryList = new ArrayList<>();
//                try {
//                    List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1), "",  false);
//                    if (predicates.size() == 0) {
//                        continue;
//                    }
//                    //query cassandra
//                    JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
//                    for (int i = 1; i < predicates.size(); ++i) {
//                        CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
//                        resultRDD = resultRDD.union(rdd);
//                    }
//
//                    entryList = resultRDD.collect()
//                            .stream()
//                            .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
//                            .entrySet()
//                            .stream()
//                            .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
//                            .collect(Collectors.toList());
//                } catch (NoHostAvailableException e) {
//                    throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
//                } catch (OperationTimedOutException | ReadTimeoutException e) {
//                    throw new TimeoutException(e.getMessage(), e.getCause());
//                } catch (QueryExecutionException e) {
//                    throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
//                } catch (Exception e) {
//                    throw new SparkException(e.getMessage(), e.getCause());
//                }
//
//                StringBuilder sb = new StringBuilder();
//                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
//                    List<CassandraRow> rows = entry.getValue();
//                    CassandraRow first = rows.get(0);
//                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
//                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
//                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");
//
//                    Map<String, String> metricMap = new HashMap<>();
//                    for (CassandraRow row : rows) {
//                        metricMap.put(row.getString("metric"), row.getString("value"));
//                    }
//                    for (String metric : metrics) {
//                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
//                    }
//                    sb.delete(sb.length() - 1, sb.length());
//                    sb.append(lineSeparator);
//                }
//                //write to file
//                fw.write(sb.toString());
//                fw.flush();
//            }
//        }
//
//        fw.close();
//    }
//
//    @Override
//    public void exportLongCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
//        if (endTime < startTime) return;
//
//        //generate time ranges
//        List<Long> timePoints = new ArrayList<>();
//        timePoints.add(startTime);
//        long splitMillis = splitHours * 60 * 60 * 1000;
//        while (startTime + splitMillis < endTime) {
//            startTime = startTime + splitMillis;
//            timePoints.add(startTime);
//        }
//        timePoints.add(endTime);
//
//        String lineSeparator = System.getProperty("line.separator");
//        FileWriter fw = new FileWriter(filePath, true);
//        //construct file header and write to file
//        StringBuilder head = new StringBuilder();
//        head.append("ID号,").append("生成时间,").append("接收时间,");
//        for (String metric : metrics) {
//            head.append(metric).append(",");
//        }
//        head.delete(head.length() - 1, head.length());
//        head.append(lineSeparator);
//        fw.write(head.toString());
//        fw.flush();
//
//        String queryFilter = (filter == null) ? "" : " and " + filter;
//        //construct data output and write to file
//        for (String host : hosts) {
//            List<String> single = new ArrayList<>();
//            single.add(host);
//            for (int j = 0; j < timePoints.size() - 1; ++j) {
//                List<Map.Entry<Long, List<CassandraRow>>> entryList = new ArrayList<>();
//                try {
//                    List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1), "", false);
//                    if (predicates.size() == 0) {
//                        continue;
//                    }
//                    //query cassandra
//                    JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
//                    for (int i = 1; i < predicates.size(); ++i) {
//                        CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_long").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
//                        resultRDD = resultRDD.union(rdd);
//                    }
//
//                    entryList = resultRDD.collect()
//                            .stream()
//                            .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
//                            .entrySet()
//                            .stream()
//                            .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
//                            .collect(Collectors.toList());
//                } catch (NoHostAvailableException e) {
//                    throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
//                } catch (OperationTimedOutException | ReadTimeoutException e) {
//                    throw new TimeoutException(e.getMessage(), e.getCause());
//                } catch (QueryExecutionException e) {
//                    throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
//                } catch (Exception e) {
//                    throw new SparkException(e.getMessage(), e.getCause());
//                }
//
//                StringBuilder sb = new StringBuilder();
//                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
//                    List<CassandraRow> rows = entry.getValue();
//                    CassandraRow first = rows.get(0);
//                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
//                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
//                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");
//
//                    Map<String, String> metricMap = new HashMap<>();
//                    for (CassandraRow row : rows) {
//                        metricMap.put(row.getString("metric"), row.getString("value"));
//                    }
//                    for (String metric : metrics) {
//                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
//                    }
//                    sb.delete(sb.length() - 1, sb.length());
//                    sb.append(lineSeparator);
//                }
//                //write to file
//                fw.write(sb.toString());
//                fw.flush();
//            }
//        }
//
//        fw.close();
//    }
//
//    @Override
//    public void exportFloatCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
//        if (endTime < startTime) return;
//
//        //generate time ranges
//        List<Long> timePoints = new ArrayList<>();
//        timePoints.add(startTime);
//        long splitMillis = splitHours * 60 * 60 * 1000;
//        while (startTime + splitMillis < endTime) {
//            startTime = startTime + splitMillis;
//            timePoints.add(startTime);
//        }
//        timePoints.add(endTime);
//
//        String lineSeparator = System.getProperty("line.separator");
//        FileWriter fw = new FileWriter(filePath, true);
//        //construct file header and write to file
//        StringBuilder head = new StringBuilder();
//        head.append("ID号,").append("生成时间,").append("接收时间,");
//        for (String metric : metrics) {
//            head.append(metric).append(",");
//        }
//        head.delete(head.length() - 1, head.length());
//        head.append(lineSeparator);
//        fw.write(head.toString());
//        fw.flush();
//
//        String queryFilter = (filter == null) ? "" : " and " + filter;
//        //construct data output and write to file
//        for (String host : hosts) {
//            List<String> single = new ArrayList<>();
//            single.add(host);
//            for (int j = 0; j < timePoints.size() - 1; ++j) {
//                List<Map.Entry<Long, List<CassandraRow>>> entryList = new ArrayList<>();
//                try {
//                    List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1), "", false);
//                    if (predicates.size() == 0) {
//                        continue;
//                    }
//                    //query cassandra
//                    JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_float").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
//                    for (int i = 1; i < predicates.size(); ++i) {
//                        CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_int").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
//                        resultRDD = resultRDD.union(rdd);
//                    }
//
//                    entryList = resultRDD.collect()
//                            .stream()
//                            .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
//                            .entrySet()
//                            .stream()
//                            .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
//                            .collect(Collectors.toList());
//                } catch (NoHostAvailableException e) {
//                    throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
//                } catch (OperationTimedOutException | ReadTimeoutException e) {
//                    throw new TimeoutException(e.getMessage(), e.getCause());
//                } catch (QueryExecutionException e) {
//                    throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
//                } catch (Exception e) {
//                    throw new SparkException(e.getMessage(), e.getCause());
//                }
//
//                StringBuilder sb = new StringBuilder();
//                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
//                    List<CassandraRow> rows = entry.getValue();
//                    CassandraRow first = rows.get(0);
//                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
//                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
//                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");
//
//                    Map<String, String> metricMap = new HashMap<>();
//                    for (CassandraRow row : rows) {
//                        metricMap.put(row.getString("metric"), row.getString("value"));
//                    }
//                    for (String metric : metrics) {
//                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
//                    }
//                    sb.delete(sb.length() - 1, sb.length());
//                    sb.append(lineSeparator);
//                }
//                //write to file
//                fw.write(sb.toString());
//                fw.flush();
//            }
//        }
//
//        fw.close();
//    }
//
//    @Override
//    public void exportDoubleCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
//        if (endTime < startTime) return;
//
//        //generate time ranges
//        List<Long> timePoints = new ArrayList<>();
//        timePoints.add(startTime);
//        long splitMillis = splitHours * 60 * 60 * 1000;
//        while (startTime + splitMillis < endTime) {
//            startTime = startTime + splitMillis;
//            timePoints.add(startTime);
//        }
//        timePoints.add(endTime);
//
//        String lineSeparator = System.getProperty("line.separator");
//        FileWriter fw = new FileWriter(filePath, true);
//        //construct file header and write to file
//        StringBuilder head = new StringBuilder();
//        head.append("ID号,").append("生成时间,").append("接收时间,");
//        for (String metric : metrics) {
//            head.append(metric).append(",");
//        }
//        head.delete(head.length() - 1, head.length());
//        head.append(lineSeparator);
//        fw.write(head.toString());
//        fw.flush();
//
//        String queryFilter = (filter == null) ? "" : " and " + filter;
//        //construct data output and write to file
//        for (String host : hosts) {
//            List<String> single = new ArrayList<>();
//            single.add(host);
//            for (int j = 0; j < timePoints.size() - 1; ++j) {
//                List<Map.Entry<Long, List<CassandraRow>>> entryList = new ArrayList<>();
//                try {
//                    List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1), "", false);
//                    if (predicates.size() == 0) {
//                        continue;
//                    }
//                    //query cassandra
//                    JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
//                    for (int i = 1; i < predicates.size(); ++i) {
//                        CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_double").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
//                        resultRDD = resultRDD.union(rdd);
//                    }
//
//                    entryList = resultRDD.collect()
//                            .stream()
//                            .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
//                            .entrySet()
//                            .stream()
//                            .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
//                            .collect(Collectors.toList());
//                } catch (NoHostAvailableException e) {
//                    throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
//                } catch (OperationTimedOutException | ReadTimeoutException e) {
//                    throw new TimeoutException(e.getMessage(), e.getCause());
//                } catch (QueryExecutionException e) {
//                    throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
//                } catch (Exception e) {
//                    throw new SparkException(e.getMessage(), e.getCause());
//                }
//
//                StringBuilder sb = new StringBuilder();
//                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
//                    List<CassandraRow> rows = entry.getValue();
//                    CassandraRow first = rows.get(0);
//                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
//                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
//                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");
//
//                    Map<String, String> metricMap = new HashMap<>();
//                    for (CassandraRow row : rows) {
//                        metricMap.put(row.getString("metric"), row.getString("value"));
//                    }
//                    for (String metric : metrics) {
//                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
//                    }
//                    sb.delete(sb.length() - 1, sb.length());
//                    sb.append(lineSeparator);
//                }
//                //write to file
//                fw.write(sb.toString());
//                fw.flush();
//            }
//        }
//
//        fw.close();
//    }
//
//    @Override
//    public void exportBooleanCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
//        if (endTime < startTime) return;
//
//        //generate time ranges
//        List<Long> timePoints = new ArrayList<>();
//        timePoints.add(startTime);
//        long splitMillis = splitHours * 60 * 60 * 1000;
//        while (startTime + splitMillis < endTime) {
//            startTime = startTime + splitMillis;
//            timePoints.add(startTime);
//        }
//        timePoints.add(endTime);
//
//        String lineSeparator = System.getProperty("line.separator");
//        FileWriter fw = new FileWriter(filePath, true);
//        //construct file header and write to file
//        StringBuilder head = new StringBuilder();
//        head.append("ID号,").append("生成时间,").append("接收时间,");
//        for (String metric : metrics) {
//            head.append(metric).append(",");
//        }
//        head.delete(head.length() - 1, head.length());
//        head.append(lineSeparator);
//        fw.write(head.toString());
//        fw.flush();
//
//        String queryFilter = (filter == null) ? "" : " and " + filter;
//        //construct data output and write to file
//        for (String host : hosts) {
//            List<String> single = new ArrayList<>();
//            single.add(host);
//            for (int j = 0; j < timePoints.size() - 1; ++j) {
//                List<Map.Entry<Long, List<CassandraRow>>> entryList = new ArrayList<>();
//                try {
//                    List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1), "", false);
//                    if (predicates.size() == 0) {
//                        continue;
//                    }
//                    //query cassandra
//                    JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0) + queryFilter);
//                    for (int i = 1; i < predicates.size(); ++i) {
//                        CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_boolean").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i) + queryFilter);
//                        resultRDD = resultRDD.union(rdd);
//                    }
//
//                    entryList = resultRDD.collect()
//                            .stream()
//                            .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
//                            .entrySet()
//                            .stream()
//                            .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
//                            .collect(Collectors.toList());
//                } catch (NoHostAvailableException e) {
//                    throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
//                } catch (OperationTimedOutException | ReadTimeoutException e) {
//                    throw new TimeoutException(e.getMessage(), e.getCause());
//                } catch (QueryExecutionException e) {
//                    throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
//                } catch (Exception e) {
//                    throw new SparkException(e.getMessage(), e.getCause());
//                }
//
//                StringBuilder sb = new StringBuilder();
//                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
//                    List<CassandraRow> rows = entry.getValue();
//                    CassandraRow first = rows.get(0);
//                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
//                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
//                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");
//
//                    Map<String, String> metricMap = new HashMap<>();
//                    for (CassandraRow row : rows) {
//                        metricMap.put(row.getString("metric"), row.getString("value"));
//                    }
//                    for (String metric : metrics) {
//                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
//                    }
//                    sb.delete(sb.length() - 1, sb.length());
//                    sb.append(lineSeparator);
//                }
//                //write to file
//                fw.write(sb.toString());
//                fw.flush();
//            }
//        }
//
//        fw.close();
//    }
//
//    @Override
//    public void exportStringCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
//        if (endTime < startTime) return;
//
//        //generate time ranges
//        List<Long> timePoints = new ArrayList<>();
//        timePoints.add(startTime);
//        long splitMillis = splitHours * 60 * 60 * 1000;
//        while (startTime + splitMillis < endTime) {
//            startTime = startTime + splitMillis;
//            timePoints.add(startTime);
//        }
//        timePoints.add(endTime);
//
//        String lineSeparator = System.getProperty("line.separator");
//        FileWriter fw = new FileWriter(filePath, true);
//        //construct file header and write to file
//        StringBuilder head = new StringBuilder();
//        head.append("ID号,").append("生成时间,").append("接收时间,");
//        for (String metric : metrics) {
//            head.append(metric).append(",");
//        }
//        head.delete(head.length() - 1, head.length());
//        head.append(lineSeparator);
//        fw.write(head.toString());
//        fw.flush();
//
//        String regex = filter.split(" ")[2];
//        //construct data output and write to file
//        for (String host : hosts) {
//            List<String> single = new ArrayList<>();
//            single.add(host);
//            for (int j = 0; j < timePoints.size() - 1; ++j) {
//                List<Map.Entry<Long, List<CassandraRow>>> entryList = new ArrayList<>();
//                try {
//                    List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1), "", false);
//                    if (predicates.size() == 0) {
//                        continue;
//                    }
//                    //query cassandra
//                    JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_text").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(0)).filter(x -> x.getString("value").matches(regex));
//                    for (int i = 1; i < predicates.size(); ++i) {
//                        JavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_text").select("host", "metric", "primary_time", "secondary_time", "value").where(predicates.get(i)).filter(x -> x.getString("value").matches(regex));
//                        resultRDD = resultRDD.union(rdd);
//                    }
//
//                    entryList = resultRDD.collect()
//                            .stream()
//                            .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
//                            .entrySet()
//                            .stream()
//                            .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
//                            .collect(Collectors.toList());
//                } catch (NoHostAvailableException e) {
//                    throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
//                } catch (OperationTimedOutException | ReadTimeoutException e) {
//                    throw new TimeoutException(e.getMessage(), e.getCause());
//                } catch (QueryExecutionException e) {
//                    throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
//                } catch (Exception e) {
//                    throw new SparkException(e.getMessage(), e.getCause());
//                }
//
//                StringBuilder sb = new StringBuilder();
//                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
//                    List<CassandraRow> rows = entry.getValue();
//                    CassandraRow first = rows.get(0);
//                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
//                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
//                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");
//
//                    Map<String, String> metricMap = new HashMap<>();
//                    for (CassandraRow row : rows) {
//                        metricMap.put(row.getString("metric"), row.getString("value"));
//                    }
//                    for (String metric : metrics) {
//                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
//                    }
//                    sb.delete(sb.length() - 1, sb.length());
//                    sb.append(lineSeparator);
//                }
//                //write to file
//                fw.write(sb.toString());
//                fw.flush();
//            }
//        }
//
//        fw.close();
//    }
//
//    @Override
//    public void exportGeoCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
//        if (endTime < startTime) return;
//
//        //generate time ranges
//        List<Long> timePoints = new ArrayList<>();
//        timePoints.add(startTime);
//        long splitMillis = splitHours * 60 * 60 * 1000;
//        while (startTime + splitMillis < endTime) {
//            startTime = startTime + splitMillis;
//            timePoints.add(startTime);
//        }
//        timePoints.add(endTime);
//
//        String lineSeparator = System.getProperty("line.separator");
//        FileWriter fw = new FileWriter(filePath, true);
//        //construct file header and write to file
//        StringBuilder head = new StringBuilder();
//        head.append("ID号,").append("生成时间,").append("接收时间,");
//        for (String metric : metrics) {
//            head.append(metric).append(",");
//        }
//        head.delete(head.length() - 1, head.length());
//        head.append(lineSeparator);
//        fw.write(head.toString());
//        fw.flush();
//
//        String queryFilter = (filter == null) ? "" : " and " + filter;
//        //construct data output and write to file
//        for (String host : hosts) {
//            List<String> single = new ArrayList<>();
//            single.add(host);
//            for (int j = 0; j < timePoints.size() - 1; ++j) {
//                List<Map.Entry<Long, List<CassandraRow>>> entryList = new ArrayList<>();
//                try {
//                    List<String> predicates = getRangeQueryPredicates(single, metrics, timePoints.get(j), timePoints.get(j + 1), "", false);
//                    if (predicates.size() == 0) {
//                        continue;
//                    }
//                    //query cassandra
//                    JavaRDD<CassandraRow> resultRDD = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_geo").select("host", "metric", "primary_time", "secondary_time", "latitude", "longitude").where(predicates.get(0) + queryFilter);
//                    for (int i = 1; i < predicates.size(); ++i) {
//                        CassandraTableScanJavaRDD<CassandraRow> rdd = javaFunctions(sparkContext).cassandraTable("sagittarius", "data_geo").select("host", "metric", "primary_time", "secondary_time", "latitude", "longitude").where(predicates.get(i) + queryFilter);
//                        resultRDD = resultRDD.union(rdd);
//                    }
//
//                    entryList = resultRDD.collect()
//                            .stream()
//                            .collect(Collectors.groupingBy(e -> e.getLong("primary_time")))
//                            .entrySet()
//                            .stream()
//                            .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
//                            .collect(Collectors.toList());
//                } catch (NoHostAvailableException e) {
//                    throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
//                } catch (OperationTimedOutException | ReadTimeoutException e) {
//                    throw new TimeoutException(e.getMessage(), e.getCause());
//                } catch (QueryExecutionException e) {
//                    throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
//                } catch (Exception e) {
//                    throw new SparkException(e.getMessage(), e.getCause());
//                }
//
//                StringBuilder sb = new StringBuilder();
//                for (Map.Entry<Long, List<CassandraRow>> entry : entryList) {
//                    List<CassandraRow> rows = entry.getValue();
//                    CassandraRow first = rows.get(0);
//                    String primaryTime = TimeUtil.date2String(first.getLong("primary_time"), TimeUtil.dateFormat1);
//                    String secondaryTime = first.getLong("secondary_time") != null ? TimeUtil.date2String(first.getLong("secondary_time"), TimeUtil.dateFormat1) : "null";
//                    sb.append(first.getString("host")).append(",").append(primaryTime).append(",").append(secondaryTime).append(",");
//
//                    Map<String, String> metricMap = new HashMap<>();
//                    for (CassandraRow row : rows) {
//                        metricMap.put(row.getString("metric"), row.getString("latitude") + "#" + row.getString("longitude"));
//                    }
//                    for (String metric : metrics) {
//                        sb.append(metricMap.getOrDefault(metric, "")).append(",");
//                    }
//                    sb.delete(sb.length() - 1, sb.length());
//                    sb.append(lineSeparator);
//                }
//                //write to file
//                fw.write(sb.toString());
//                fw.flush();
//            }
//        }
//
//        fw.close();
//    }

    private List<AggregationData> getAggResult2(String filter) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        List<AggregationData> result;
        try{
            SimpleStatement statement = new SimpleStatement("select * from data_aggregation where " + filter);
            ResultSet resultSet = session.execute(statement);
            Mapper<AggregationData> mapper = mappingManager.mapper(AggregationData.class);
            result = mapper.map(resultSet).all();
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }

        return result;
    }

    private String getAggregationDataPredicate2(String host, String metric, long startHour, long endHour, String filter, AggregationType aggregationType) {
        // TODO: 17-4-6 what if string data and geo data?
        String predicate = null;
        switch (aggregationType) {
            case MAX: {
                String queryFilter = filter == null ? "" : " and " + filter.replaceAll("value", "max_value");
                predicate = ("host = \'" + host + "\' and metric = \'" + metric + "\' and time_slice >= " + startHour + " and time_slice < " + endHour + queryFilter);
                break;
            }
            case MIN: {
                String queryFilter = filter == null ? "" : " and " + filter.replaceAll("value", "min_value");
                predicate = ("host = \'" + host + "\' and metric = \'" + metric + "\' and time_slice >= " + startHour + " and time_slice < " + endHour + queryFilter);
                break;
            }
            case COUNT: {
                String queryFilter = filter == null ? "" : " and " + filter.replaceAll("value", "max_value") + " and " + filter.replaceAll("value", "min_value");
                predicate = ("host = \'" + host + "\' and metric = \'" + metric + "\' and time_slice >= " + startHour + " and time_slice < " + endHour + queryFilter);
                break;
            }
            case SUM: {
                String queryFilter = filter == null ? "" : " and " + filter.replaceAll("value", "max_value") + " and " + filter.replaceAll("value", "min_value");
                predicate = ("host = \'" + host + "\' and metric = \'" + metric + "\' and time_slice >= " + startHour + " and time_slice < " + endHour + queryFilter);
            }
        }
        return predicate;
    }

    Map<String, Map<String, Double>> getNumericRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType, ValueType valueType) {
        Map<String, Map<String, Double>> result = null;
        switch (valueType) {
            case INT: {
                try {
                    result = getIntRange(hosts, metrics, startTime, endTime, filter, aggregationType);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            }
            case LONG: {
                try {
                    result = getLongRange(hosts, metrics, startTime, endTime, filter, aggregationType);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            }
            case FLOAT: {
                try {
                    result = getFloatRange(hosts, metrics, startTime, endTime, filter, aggregationType);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            }
            case DOUBLE: {
                try {
                    result = getDoubleRange(hosts, metrics, startTime, endTime, filter, aggregationType);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            }
        }
        return result;
    }

    private Double getSingleAggregationResult2(String host, String metric, long startTime, long endTime, String filter, AggregationType aggregationType, ValueType valueType) throws com.sagittarius.exceptions.NoHostAvailableException, com.sagittarius.exceptions.QueryExecutionException, TimeoutException {

        Double result = 0d;
        ArrayList<String> hosts = new ArrayList<>();
        hosts.add(host);
        ArrayList<String> metrics = new ArrayList<>();
        metrics.add(metric);

        long m_startTime = startTime + 28800000;
        long m_endTime = endTime + 28800000;

        long startHour = (m_startTime % 86400000 == 0) ? (m_startTime / 86400000) : (m_startTime / 86400000 + 1);
        long endHour = m_endTime / 86400000;

        long queryStartTime = startHour * 86400000 - 28800000;
        long queryEndTime = endHour * 86400000 - 28800000;
        switch (aggregationType) {
            case MAX: {
                //suggestion : query time fits whole hour so there's no need to computer first and last interval
                double firstIntervalResult;
                if (queryStartTime > startTime) {
                    Map<String, Map<String, Double>> firstIntervalMap = getNumericRange(hosts, metrics, startTime, queryStartTime, filter, aggregationType, valueType);
                    firstIntervalResult = firstIntervalMap.isEmpty() ? 0d : firstIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    firstIntervalResult = 0d;
                }

                double lastIntervalResult;
                if (endTime > queryEndTime) {
                    Map<String, Map<String, Double>> lastIntervalMap = getNumericRange(hosts, metrics, queryEndTime, endTime, filter, aggregationType, valueType);
                    lastIntervalResult = lastIntervalMap.isEmpty() ? 0d : lastIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    lastIntervalResult = 0d;
                }
                result = Math.max(firstIntervalResult, lastIntervalResult);
                List<AggregationData> totoalResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, null, aggregationType));
                List<AggregationData> ResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, filter, aggregationType) + " ALLOW FILTERING");
                HashSet<Long> totalTimeSlices = new HashSet<Long>();
                HashSet<Long> usedTimeSlices = new HashSet<Long>();
                for (AggregationData r : totoalResultSet) {
                    totalTimeSlices.add(r.getTimeSlice());
                }
                for (AggregationData r : ResultSet) {
                    usedTimeSlices.add(r.getTimeSlice());
                    result = Math.max(result, r.getMaxValue());
                }
                totalTimeSlices.removeAll(usedTimeSlices);
                for (long time : totalTimeSlices) {
                    Map<String, Map<String, Double>> intervalMap = getNumericRange(hosts, metrics, time * 86400000 - 28800000, time * 86400000 + 86400000 - 28800000, filter, aggregationType, valueType);
                    double intervalResult = intervalMap.isEmpty() ? 0d : intervalMap.get(host).get(metric);
                    result = Math.max(result, intervalResult);
                }
                break;
            }
            case MIN: {
                //suggestion : query time fits whole hour so there's no need to computer first and last interval
                double firstIntervalResult;
                if (queryStartTime > startTime) {
                    Map<String, Map<String, Double>> firstIntervalMap = getNumericRange(hosts, metrics, startTime, queryStartTime, filter, aggregationType, valueType);
                    firstIntervalResult = firstIntervalMap.isEmpty() ? Long.MAX_VALUE : firstIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    firstIntervalResult = Long.MAX_VALUE;
                }

                double lastIntervalResult;
                if (endTime > queryEndTime) {
                    Map<String, Map<String, Double>> lastIntervalMap = getNumericRange(hosts, metrics, queryEndTime, endTime, filter, aggregationType, valueType);
                    lastIntervalResult = lastIntervalMap.isEmpty() ? Long.MAX_VALUE : lastIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    lastIntervalResult = Long.MAX_VALUE;
                }
                result = Math.min(firstIntervalResult, lastIntervalResult);
                List<AggregationData> totoalResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, null, aggregationType));
                List<AggregationData> ResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, filter, aggregationType) + " ALLOW FILTERING");
                HashSet<Long> totalTimeSlices = new HashSet<Long>();
                HashSet<Long> usedTimeSlices = new HashSet<Long>();
                for (AggregationData r : totoalResultSet) {
                    totalTimeSlices.add(r.getTimeSlice());
                }
                for (AggregationData r : ResultSet) {
                    usedTimeSlices.add(r.getTimeSlice());
                    result = Math.min(result, r.getMinValue());
                }
                totalTimeSlices.removeAll(usedTimeSlices);
                for (long time : totalTimeSlices) {
                    Map<String, Map<String, Double>> intervalMap = getNumericRange(hosts, metrics, time * 86400000 - 28800000, time * 86400000 + 86400000 - 28800000, filter, aggregationType, valueType);
                    double intervalResult = intervalMap.isEmpty() ? Long.MAX_VALUE : intervalMap.get(host).get(metric);
                    result = Math.min(result, intervalResult);
                }
                break;
            }
            case COUNT: {
                //suggestion : query time fits whole hour so there's no need to computer first and last interval
                double firstIntervalResult;
                if (queryStartTime > startTime) {
                    Map<String, Map<String, Double>> firstIntervalMap = getNumericRange(hosts, metrics, startTime, queryStartTime, filter, aggregationType, valueType);
                    firstIntervalResult = firstIntervalMap.isEmpty() ? 0d : firstIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    firstIntervalResult = 0d;
                }

                double lastIntervalResult;
                if (endTime > queryEndTime) {
                    Map<String, Map<String, Double>> lastIntervalMap = getNumericRange(hosts, metrics, queryEndTime, endTime, filter, aggregationType, valueType);
                    lastIntervalResult = lastIntervalMap.isEmpty() ? 0d : lastIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    lastIntervalResult = 0d;
                }
                result = firstIntervalResult + lastIntervalResult;
                List<AggregationData> totoalResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, null, aggregationType));
                List<AggregationData> ResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, filter, aggregationType) + " ALLOW FILTERING");
                HashSet<Long> totalTimeSlices = new HashSet<Long>();
                HashSet<Long> usedTimeSlices = new HashSet<Long>();
                for (AggregationData r : totoalResultSet) {
                    totalTimeSlices.add(r.getTimeSlice());
                }
                for (AggregationData r : ResultSet) {
                    usedTimeSlices.add(r.getTimeSlice());
                    result += r.getCountValue();
                }
                totalTimeSlices.removeAll(usedTimeSlices);
                for (long time : totalTimeSlices) {
                    Map<String, Map<String, Double>> intervalMap = getNumericRange(hosts, metrics, time * 86400000 - 28800000, time * 86400000 + 86400000 - 28800000, filter, aggregationType, valueType);
                    double intervalResult = intervalMap.isEmpty() ? 0d : intervalMap.get(host).get(metric);
                    result += intervalResult;
                }
                break;
            }
            case SUM: {
                //suggestion : query time fits whole hour so there's no need to computer first and last interval
                double firstIntervalResult;
                if (queryStartTime > startTime) {
                    Map<String, Map<String, Double>> firstIntervalMap = getNumericRange(hosts, metrics, startTime, queryStartTime, filter, aggregationType, valueType);
                    firstIntervalResult = firstIntervalMap.isEmpty() ? 0d : firstIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    firstIntervalResult = 0d;
                }

                double lastIntervalResult;
                if (endTime > queryEndTime) {
                    Map<String, Map<String, Double>> lastIntervalMap = getNumericRange(hosts, metrics, queryEndTime, endTime, filter, aggregationType, valueType);
                    lastIntervalResult = lastIntervalMap.isEmpty() ? 0d : lastIntervalMap.get(host).get(metric);
                } else {
                    // TODO: 17-4-5 a proper initial value
                    lastIntervalResult = 0d;
                }
                result = firstIntervalResult + lastIntervalResult;
                List<AggregationData> totoalResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, null, aggregationType));
                List<AggregationData> ResultSet = getAggResult2(getAggregationDataPredicate2(host, metric, startHour, endHour, filter, aggregationType) + " ALLOW FILTERING");
                HashSet<Long> totalTimeSlices = new HashSet<Long>();
                HashSet<Long> usedTimeSlices = new HashSet<Long>();
                for (AggregationData r : totoalResultSet) {
                    totalTimeSlices.add(r.getTimeSlice());
                }
                for (AggregationData r : ResultSet) {
                    usedTimeSlices.add(r.getTimeSlice());
                    result += r.getSumValue();
                }
                totalTimeSlices.removeAll(usedTimeSlices);
                for (long time : totalTimeSlices) {
                    Map<String, Map<String, Double>> intervalMap = getNumericRange(hosts, metrics, time * 86400000 - 28800000, time * 86400000 + 86400000 - 28800000, filter, aggregationType, valueType);
                    double intervalResult = intervalMap.isEmpty() ? 0d : intervalMap.get(host).get(metric);
                    result += intervalResult;
                }
                break;
            }
            case AVG: {
                double s = getSingleAggregationResult2(host, metric, startTime, endTime, filter, AggregationType.SUM, valueType);
                double c = getSingleAggregationResult2(host, metric, startTime, endTime, filter, AggregationType.COUNT, valueType);
                result = s / c;
                break;
            }
        }
        return result;
    }

    public Map<String, Map<String, Double>> getAggregationRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws com.sagittarius.exceptions.NoHostAvailableException, com.sagittarius.exceptions.QueryExecutionException, TimeoutException {
        //make sure that data has been pre-aggregated
        Map<String, Map<String, Double>> result = new HashMap<>();
        for (String host : hosts) {
            Map<String, Double> hostResult = new HashMap<>();
            for (String metric : metrics) {
                hostResult.put(metric, getSingleAggregationResult2(host, metric, startTime, endTime, filter, aggregationType, getValueType(host,metric)));
            }
            result.put(host, hostResult);
        }
        return result;
    }

    public void preAggregateFunction(List<String> hosts, List<String> metrics, long startTime, long endTime, Writer writer) throws com.sagittarius.exceptions.NoHostAvailableException, com.sagittarius.exceptions.QueryExecutionException, TimeoutException {


        try{
            long startTimeHour = (startTime+28800000) / 86400000;
            long endTimeHour = (endTime+28800000) / 86400000;
            while (startTimeHour < endTimeHour) {
                long queryStartTime = startTimeHour * 86400000 - 28800000;
                long queryEndTime = queryStartTime + 86400000;
                for (String host : hosts) {
                    for (String metric : metrics) {
                        ValueType valueType = getValueType(host, metric);
                        Class classType = getClassType(valueType);
                        String tablename = getTableByType(valueType);
                        ArrayList<String> queryHost = new ArrayList<>();
                        queryHost.add(host);
                        ArrayList<String> queryMetric = new ArrayList<>();
                        queryMetric.add(metric);
                        String filter = getRangeQueryPredicates(queryHost, queryMetric, queryStartTime, queryEndTime, "", false).get(0);
                        SimpleStatement maxStatement = new SimpleStatement("select max(value) from " + tablename + " where " + filter);
                        SimpleStatement minStatement = new SimpleStatement("select min(value) from " + tablename + " where " + filter);
                        SimpleStatement countStatement = new SimpleStatement("select count(value) from " + tablename + " where " + filter);
                        SimpleStatement sumStatement = new SimpleStatement("select sum(value) from " + tablename + " where " + filter);
                        if(valueType == ValueType.STRING || valueType == ValueType.BOOLEAN || valueType == ValueType.GEO){
                            ResultSet resultSet = session.execute(countStatement);
                            long countResult = resultSet.all().get(0).getLong(0);
                            if (countResult > 0) {
                                writer.insert(host, metric, startTimeHour, 0, 0, (double)countResult, 0);
                            }
                        }
                        else if(valueType == ValueType.INT){
                            int maxResult = session.execute(maxStatement).one().getInt(0);
                            int minResult = session.execute(minStatement).one().getInt(0);
                            long countResult = session.execute(countStatement).one().getLong(0);
                            int sumResult = session.execute(sumStatement).one().getInt(0);
                            if(countResult > 0){
                                writer.insert(host, metric, startTimeHour, maxResult, minResult, countResult, sumResult);
                            }
                        }else if(valueType == ValueType.LONG){
                            long maxResult = session.execute(maxStatement).one().getLong(0);
                            long minResult = session.execute(minStatement).one().getLong(0);
                            long countResult = session.execute(countStatement).one().getLong(0);
                            long sumResult = session.execute(sumStatement).one().getLong(0);
                            if(countResult > 0){
                                writer.insert(host, metric, startTimeHour, maxResult, minResult, countResult, sumResult);
                            }
                        }else if(valueType == ValueType.FLOAT){
                            float maxResult = session.execute(maxStatement).one().getFloat(0);
                            float minResult = session.execute(minStatement).one().getFloat(0);
                            long countResult = session.execute(countStatement).one().getLong(0);
                            float sumResult = session.execute(sumStatement).one().getFloat(0);
                            if(countResult > 0){
                                writer.insert(host, metric, startTimeHour, maxResult, minResult, countResult, sumResult);
                            }
                        }else if(valueType == ValueType.DOUBLE){
                            double maxResult = session.execute(maxStatement).one().getDouble(0);
                            double minResult = session.execute(minStatement).one().getDouble(0);
                            long countResult = session.execute(countStatement).one().getLong(0);
                            double sumResult = session.execute(sumStatement).one().getDouble(0);
                            if(countResult > 0){
                                writer.insert(host, metric, startTimeHour, maxResult, minResult, countResult, sumResult);
                            }
                        }
                    }
                }
                startTimeHour += 1;
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | ReadTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    private static Class getClassType(ValueType valueType){
        if(valueType == ValueType.INT){
            return Integer.class;
        }
        if(valueType == ValueType.LONG){
            return Long.class;
        }
        if(valueType == ValueType.FLOAT){
            return Float.class;
        }
        if(valueType == ValueType.DOUBLE){
            return Double.class;
        }
        if(valueType == ValueType.STRING){
            return String.class;
        }
        if(valueType == ValueType.BOOLEAN){
            return Boolean.class;
        }
        if(valueType == ValueType.GEO){
            return Float.class;
        }
        return Object.class;
    }

//    public void preAggregateFunction2(List<String> hosts, List<String> metrics, long startTime, long endTime, SagittariusWriter writer) throws com.sagittarius.exceptions.NoHostAvailableException, com.sagittarius.exceptions.QueryExecutionException, TimeoutException {
//
//        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
//        for(HostMetric hostMetric : hostMetrics){
//            String host = hostMetric.getHost();
//            String metric = hostMetric.getMetric();
//            TimePartition timePartition = hostMetric.getTimePartition();
//            long time_slice = -1;
//            switch (timePartition){
//                case DAY:{
//                    time_slice = startTime / 86400000L;
//                    break;
//                }
//                case WEEK:{
//                    LocalDateTime time = LocalDateTime.ofEpochSecond(startTime/1000, 0, ZoneOffset.ofHours(8));
//                    int year = time.getYear();
//                    int week = time.get(ALIGNED_WEEK_OF_YEAR) - 1;
//                    time_slice = year * 53 + week;
//                    break;
//                }
//                case MONTH:{
//                    LocalDateTime time = LocalDateTime.ofEpochSecond(startTime/1000, 0, ZoneOffset.ofHours(8));
//                    int year = time.getYear();
//                    int month = time.getMonthValue() - 1;
//                    time_slice = year * 12 + month;
//                    break;
//                }
//                case YEAR:{
//                    LocalDateTime time = LocalDateTime.ofEpochSecond(startTime/1000, 0, ZoneOffset.ofHours(8));
//                    time_slice = time.getYear();
//                    break;
//                }
//            }
//            if(time_slice < 0){
//                continue;
//            }
//            List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
//            ValueType valueType = hostMetric.getValueType();
//            PreparedStatement aggStatement = null;
//            switch (valueType){
//                case INT:
//                    aggStatement = aggIntStatement;
//                    for (String timeSlice : timeSlices){
//                        BoundStatement statement = aggStatement.bind(host, metric, timeSlice);
//                        ResultSet resultSet = session.execute(statement);
//                        double countResult = (double) (resultSet.all().get(0).getLong(0));
//                        if(countResult == 0){
//                            time_slice += 1;
//                            continue;
//                        }
//                        double maxResult = (double) (resultSet.all().get(0).getInt(1));
//                        double minResult = (double) (resultSet.all().get(0).getInt(2));
//                        double sumResult = (double) (resultSet.all().get(0).getInt(3));
//                        writer.insert(host, metric, time_slice, maxResult, minResult, countResult, sumResult);
//                        time_slice += 1;
//                    }
//                    break;
//                case LONG:
//                    aggStatement = aggLongStatement;
//                    break;
//                case FLOAT:
//                    aggStatement = aggFloatStatement;
//                    for (String timeSlice : timeSlices){
//                        BoundStatement statement = aggStatement.bind(host, metric, timeSlice);
//                        ResultSet resultSet = session.execute(statement);
//                        double countResult = (double) (resultSet.all().get(0).getLong(0));
//                        if(countResult == 0){
//                            time_slice += 1;
//                            continue;
//                        }
//                        double maxResult = (double) (resultSet.all().get(0).getFloat(1));
//                        double minResult = (double) (resultSet.all().get(0).getFloat(2));
//                        double sumResult = (double) (resultSet.all().get(0).getFloat(3));
//                        writer.insert(host, metric, time_slice, maxResult, minResult, countResult, sumResult);
//                        time_slice += 1;
//                    }
//                    break;
//                case DOUBLE:
//                    aggStatement = aggDoubleStatement;
//                    break;
//                case STRING:
//                    aggStatement = aggStringStatement;
//                    break;
//                case BOOLEAN:
//                    aggStatement = aggBooleanStatement;
//                    break;
//                case GEO:
//                    aggStatement = aggGeoStatement;
//                    break;
//            }
//            if(aggStatement == null){
//                continue;
//            }
//
//        }
//    }
}