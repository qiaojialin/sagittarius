package com.sagittarius.read;


import com.sagittarius.bean.common.ValueType;
import com.sagittarius.bean.query.*;
import com.sagittarius.bean.result.*;
import com.sagittarius.bean.table.HostMetric;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.write.Writer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public interface Reader {
    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get map of valueType.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of valueType, the key is valueType, the value is a map containing set of hosts and set of metrics which are of this valueType
     */
    Map<ValueType, Map<String, Set<String>>> getValueType(List<String> hosts, List<String> metrics) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given host and metric, get the corresponding valueType.
     *
     * @param host   host
     * @param metric metric
     * @return valueType corresponding to that specified metric
     */
    ValueType getValueType(String host, String metric) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get IntPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of IntPoints at the query time, the key is  host name, the value is list of IntPoints related to that host
     */
    Map<String, Map<String, IntPoint>> getIntPoint(List<String> hosts, List<String> metrics, long time) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get LongPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of LongPoints at the query time, the key is  host name, the value is list of LongPoints related to that host
     */
    Map<String, Map<String, LongPoint>> getLongPoint(List<String> hosts, List<String> metrics, long time) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get FloatPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of FloatPoints at the query time, the key is  host name, the value is list of FloatPoints related to that host
     */
    Map<String, Map<String, FloatPoint>> getFloatPoint(List<String> hosts, List<String> metrics, long time) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get DoublePoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of DoublePoints at the query time, the key is  host name, the value is list of DoublePoints related to that host
     */
    Map<String, Map<String, DoublePoint>> getDoublePoint(List<String> hosts, List<String> metrics, long time) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get BooleanPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of BooleanPoints at the query time, the key is  host name, the value is list of BooleanPoints related to that host
     */
    Map<String, Map<String, BooleanPoint>> getBooleanPoint(List<String> hosts, List<String> metrics, long time) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get StringPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of StringPoints at the query time, the key is  host name, the value is list of StringPoints related to that host
     */
    Map<String, Map<String, StringPoint>> getStringPoint(List<String> hosts, List<String> metrics, long time) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get GeoPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of GeoPoints at the query time, the key is  host name, the value is list of GeoPoints related to that host
     */
    Map<String, Map<String, GeoPoint>> getGeoPoint(List<String> hosts, List<String> metrics, long time) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
/**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get GeoPoints at the query time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @param time    query time
     * @return map of BlobPoints at the query time, the key is  host name, the value is list of BlobPoints related to that host
     */
    Map<String, Map<String, BlobPoint>> getBlobPoint(List<String> hosts, List<String> metrics, long time) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given host and metric , get a IntPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a IntPoint
     */
    IntPoint getFuzzyIntPoint(String host, String metric, long time, Shift shift) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    IntPoint getFuzzyIntPoint(String host, String metric, long time, Shift shift, long limit) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given host and metric , get a LongPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a LongPoint
     */
    LongPoint getFuzzyLongPoint(String host, String metric, long time, Shift shift) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    LongPoint getFuzzyLongPoint(String host, String metric, long time, Shift shift, long limit) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given host and metric , get a FloatPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a FloatPoint
     */
    FloatPoint getFuzzyFloatPoint(String host, String metric, long time, Shift shift) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    FloatPoint getFuzzyFloatPoint(String host, String metric, long time, Shift shift, long limit) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given host and metric , get a DoublePoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a DoublePoint
     */
    DoublePoint getFuzzyDoublePoint(String host, String metric, long time, Shift shift) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    DoublePoint getFuzzyDoublePoint(String host, String metric, long time, Shift shift, long limit) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given host and metric , get a BooleanPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a BooleanPoint
     */
    BooleanPoint getFuzzyBooleanPoint(String host, String metric, long time, Shift shift) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    BooleanPoint getFuzzyBooleanPoint(String host, String metric, long time, Shift shift, long limit) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given host and metric , get a StringPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a StringPoint
     */
    StringPoint getFuzzyStringPoint(String host, String metric, long time, Shift shift) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    StringPoint getFuzzyStringPoint(String host, String metric, long time, Shift shift, long limit) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given host and metric , get a GeoPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a GeoPoint
     */
    GeoPoint getFuzzyGeoPoint(String host, String metric, long time, Shift shift) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    GeoPoint getFuzzyGeoPoint(String host, String metric, long time, Shift shift, long limit) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
/**
     * given host and metric , get a BlobPoint at the query time, if there isn't point at that time, will search point according to shift option.
     * the supported shifts are:
     * <br>
     * <br>       BEFORE -- search backward for a point nearest to the query time
     * <br>       AFTER -- search forward for a point nearest to the query time
     * <br>       NEAREST -- search backward and forward for a point nearest to the query time
     *
     * @param host   a hosts,namely device
     * @param metric a metric, namely sensor
     * @param time   query time
     * @param shift  shift option, can be BEFORE, AFTER, NEAREST
     * @return a BlobPoint
     */
    BlobPoint getFuzzyBlobPoint(String host, String metric, long time, Shift shift) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    BlobPoint getFuzzyBlobPoint(String host, String metric, long time, Shift shift, long limit) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get IntPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of IntPoints at the latest time, the key is  host name, the value is list of IntPoints related to that host
     */
    Map<String, Map<String, IntPoint>> getIntLatest(List<String> hosts, List<String> metrics) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, IntPoint>> getIntLatest(List<String> hosts, List<String> metrics, Predicate<Integer> filter) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, List<IntPoint>>> getIntLatest(List<String> hosts, List<String> metrics, int num) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get LongPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of LongPoints at the latest time, the key is  host name, the value is list of LongPoints related to that host
     */
    Map<String, Map<String, LongPoint>> getLongLatest(List<String> hosts, List<String> metrics) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, LongPoint>> getLongLatest(List<String> hosts, List<String> metrics, Predicate<Long> filter) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, List<LongPoint>>> getLongLatest(List<String> hosts, List<String> metrics, int num) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get FloatPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of FloatPoints at the latest time, the key is  host name, the value is list of FloatPoints related to that host
     */
    Map<String, Map<String, FloatPoint>> getFloatLatest(List<String> hosts, List<String> metrics) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, FloatPoint>> getFloatLatest(List<String> hosts, List<String> metrics, Predicate<Float> filter) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, List<FloatPoint>>> getFloatLatest(List<String> hosts, List<String> metrics, int num) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get DoublePoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of DoublePoints at the latest time, the key is  host name, the value is list of DoublePoints related to that host
     */
    Map<String, Map<String, DoublePoint>> getDoubleLatest(List<String> hosts, List<String> metrics) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, DoublePoint>> getDoubleLatest(List<String> hosts, List<String> metrics, Predicate<Double> filter) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, List<DoublePoint>>> getDoubleLatest(List<String> hosts, List<String> metrics, int num) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get BooleanPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of BooleanPoints at the latest time, the key is  host name, the value is list of BooleanPoints related to that host
     */
    Map<String, Map<String, BooleanPoint>> getBooleanLatest(List<String> hosts, List<String> metrics) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, BooleanPoint>> getBooleanLatest(List<String> hosts, List<String> metrics, Predicate<Boolean> filter) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, List<BooleanPoint>>> getBooleanLatest(List<String> hosts, List<String> metrics, int num) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get StringPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of StringPoints at the latest time, the key is  host name, the value is list of StringPoints related to that host
     */
    Map<String, Map<String, StringPoint>> getStringLatest(List<String> hosts, List<String> metrics) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, StringPoint>> getStringLatest(List<String> hosts, List<String> metrics, Predicate<String> filter) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, List<StringPoint>>> getStringLatest(List<String> hosts, List<String> metrics, int num) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get GeoPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of GeoPoints at the latest time, the key is  host name, the value is list of GeoPoints related to that host
     */
    Map<String, Map<String, GeoPoint>> getGeoLatest(List<String> hosts, List<String> metrics) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, GeoPoint>> getGeoLatest(List<String> hosts, List<String> metrics, Predicate<Float> latitudeFilter, Predicate<Float> longitudeFilter) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, List<GeoPoint>>> getGeoLatest(List<String> hosts, List<String> metrics, int num) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get BlobPoints at the latest time.
     *
     * @param hosts   lists of hosts
     * @param metrics lists of metrics
     * @return map of BlobPoints at the latest time, the key is  host name, the value is list of GeoPoints related to that host
     */
    Map<String, Map<String, BlobPoint>> getBlobLatest(List<String> hosts, List<String> metrics) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, BlobPoint>> getBlobLatest(List<String> hosts, List<String> metrics, Predicate<ByteBuffer> filter) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
    Map<String, Map<String, List<BlobPoint>>> getBlobLatest(List<String> hosts, List<String> metrics, int num) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get IntPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of IntPoints, the key is  host name, the value is list of IntPoints related to that host
     */
    Map<String, Map<String, List<IntPoint>>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get LongPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of LongPoints, the key is  host name, the value is list of LongPoints related to that host
     */
    Map<String, Map<String, List<LongPoint>>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get FloatPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of FloatPoints, the key is  host name, the value is list of FloatPoints related to that host
     */
    Map<String , Map<String, List<FloatPoint>>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get DoublePoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of DoublePoints, the key is  host name, the value is list of DoublePoints related to that host
     */
    Map<String, Map<String, List<DoublePoint>>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get BooleanPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of BooleanPoints, the key is  host name, the value is list of BooleanPoints related to that host
     */
    Map<String, Map<String, List<BooleanPoint>>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get StringPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of StringPoints, the key is  host name, the value is list of StringPoints related to that host
     */
    Map<String, Map<String, List<StringPoint>>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    /**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get GeoPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of GeoPoints, the key is  host name, the value is list of GeoPoints related to that host
     */
    Map<String, Map<String, List<GeoPoint>>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;
/**
     * given hosts list and metrics list,each host is associated with the same list of metrics , get BlobPoints in the given time range.
     *
     * @param hosts     lists of hosts
     * @param metrics   lists of metrics
     * @param startTime start time of the range
     * @param endTime   end time of the range
     * @return map of BlobPoints, the key is  host name, the value is list of BlobPoints related to that host
     */
    Map<String, Map<String, List<BlobPoint>>> getBlobRange(List<String> hosts, List<String> metrics, long startTime, long endTime, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, List<IntPoint>>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, List<LongPoint>>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, List<FloatPoint>>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, List<DoublePoint>>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, List<BooleanPoint>>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, List<StringPoint>>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, List<GeoPoint>>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, List<BlobPoint>>> getBlobRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, boolean desc) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, Double>> getIntRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, Double>> getLongRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, Double>> getFloatRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, Double>> getDoubleRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, Double>> getBooleanRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, Double>> getStringRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, Double>> getGeoRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

    Map<String, Map<String, Double>> getBlobRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws NoHostAvailableException, TimeoutException, QueryExecutionException;

//    void exportIntCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, NoHostAvailableException, TimeoutException, QueryExecutionException;
//
//    void exportLongCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, NoHostAvailableException, TimeoutException, QueryExecutionException;
//
//    void exportFloatCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, NoHostAvailableException, TimeoutException, QueryExecutionException;
//
//    void exportDoubleCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, NoHostAvailableException, TimeoutException, QueryExecutionException;
//
//    void exportBooleanCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, NoHostAvailableException, TimeoutException, QueryExecutionException;
//
//    void exportStringCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, NoHostAvailableException, TimeoutException, QueryExecutionException;
//
//    void exportGeoCSV(List<String> hosts, List<String> metrics, long startTime, long endTime, int splitHours, String filter, String filePath) throws IOException, NoHostAvailableException, TimeoutException, QueryExecutionException;

    void preAggregateFunction(List<String> hosts, List<String> metrics, long startTime, long endTime, Writer writer) throws com.sagittarius.exceptions.NoHostAvailableException, com.sagittarius.exceptions.QueryExecutionException, TimeoutException;

    Map<String, Map<String, Double>> getAggregationRange(List<String> hosts, List<String> metrics, long startTime, long endTime, String filter, AggregationType aggregationType) throws NoHostAvailableException, QueryExecutionException, TimeoutException;

    List<HostMetric> getHostMetrics(List<String> hosts, List<String> metrics);
    }
