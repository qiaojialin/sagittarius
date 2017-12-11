package com.sagittarius.write;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.sagittarius.bean.common.*;
import com.sagittarius.bean.table.*;
import com.sagittarius.cache.Cache;
import com.sagittarius.exceptions.*;
import com.sagittarius.read.internals.QueryStatement;
import com.sagittarius.util.TimeUtil;
import com.sagittarius.write.internals.Monitor;
import com.sagittarius.write.internals.RecordAccumulator;
import com.sagittarius.write.internals.Sender;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.*;

import static com.datastax.driver.mapping.Mapper.Option.saveNullFields;

public class SagittariusWriter implements Writer {
    private Session session;
    private MappingManager mappingManager;
    private Cache<HostMetricPair, TypePartitionPair> cache;
    private Map<HostMetricPair, Latest> latestData = new HashMap<>();
    private RecordAccumulator accumulator;
    private Sender sender;
    private boolean autoBatch;
    private PreparedStatement preIntStatement;
    private PreparedStatement preLongStatement;
    private PreparedStatement preFloatStatement;
    private PreparedStatement preDoubleStatement;
    private PreparedStatement preStringStatement;
    private PreparedStatement preBooleanStatement;
    private PreparedStatement preBlobStatement;
    private PreparedStatement preGeoStatement;
    private PreparedStatement preIntStatementWithoutST;
    private PreparedStatement preLongStatementWithoutST;
    private PreparedStatement preFloatStatementWithoutST;
    private PreparedStatement preDoubleStatementWithoutST;
    private PreparedStatement preStringStatementWithoutST;
    private PreparedStatement preBooleanStatementWithoutST;
    private PreparedStatement preGeoStatementWithoutST;
    private PreparedStatement preBlobStatementWithoutST;
    private PreparedStatement preAggreStatement;
    private PreparedStatement preLatestStatement;

    private PreparedStatement deleteIntStatement;
    private PreparedStatement deleteLongStatement;
    private PreparedStatement deleteFloatStatement;
    private PreparedStatement deleteDoubleStatement;
    private PreparedStatement deleteStringStatement;
    private PreparedStatement deleteBooleanStatement;
    private PreparedStatement deleteBlobStatement;
    private PreparedStatement deleteGeoStatement;

    public SagittariusWriter(Session session, MappingManager mappingManager, Cache<HostMetricPair, TypePartitionPair> cache) {
        this.session = session;
        this.mappingManager = mappingManager;
        this.cache = cache;
        this.autoBatch = false;
        preIntStatement = session.prepare("insert into data_int (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preLongStatement = session.prepare("insert into data_long (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preFloatStatement = session.prepare("insert into data_float (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preDoubleStatement = session.prepare("insert into data_double (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preStringStatement = session.prepare("insert into data_text (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preBooleanStatement = session.prepare("insert into data_boolean (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
//        preBlobStatement = session.prepare("insert into data_blob (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preGeoStatement = session.prepare("insert into data_geo (host, metric, time_slice, primary_time, secondary_time, latitude, longitude) values (:host, :metric, :ts, :pt, :st, :la, :lo)");

        preIntStatementWithoutST = session.prepare("insert into data_int (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preLongStatementWithoutST = session.prepare("insert into data_long (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preFloatStatementWithoutST = session.prepare("insert into data_float (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preDoubleStatementWithoutST = session.prepare("insert into data_double (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preStringStatementWithoutST = session.prepare("insert into data_text (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preBooleanStatementWithoutST = session.prepare("insert into data_boolean (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
//        preBlobStatementWithoutST = session.prepare("insert into data_blob (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preGeoStatementWithoutST = session.prepare("insert into data_geo (host, metric, time_slice, primary_time, latitude, longitude) values (:host, :metric, :ts, :pt, :la, :lo)");

        preLatestStatement = session.prepare("insert into latest (host, metric, time_slice) values (:host, :metric, :time_slice)");

        preAggreStatement = session.prepare("insert into data_aggregation (host, metric, time_slice, max_value, min_value, count_value, sum_value) values (:host, :metric, :ts, :max, :min, :cnt, :sum)");

        deleteIntStatement = session.prepare("delete from data_int where host = :h and metric = :m and time_slice = :t");
        deleteLongStatement = session.prepare("delete from data_long where host = :h and metric = :m and time_slice = :t");
        deleteFloatStatement = session.prepare("delete from data_float where host = :h and metric = :m and time_slice = :t");
        deleteDoubleStatement = session.prepare("delete from data_double where host = :h and metric = :m and time_slice = :t");
        deleteStringStatement = session.prepare("delete from data_text where host = :h and metric = :m and time_slice = :t");
        deleteBooleanStatement = session.prepare("delete from data_boolean where host = :h and metric = :m and time_slice = :t");
//        deleteBlobStatement = session.prepare("delete from data_boolean where host = :h and metric = :m and time_slice = :t");
        deleteGeoStatement = session.prepare("delete from data_geo where host = :h and metric = :m and time_slice = :t");
    }

//    public SagittariusWriter(Session session, MappingManager mappingManager, Cache<HostMetricPair, TypePartitionPair> cache, int batchSize, int lingerMs) {
//        this.session = session;
//        this.mappingManager = mappingManager;
//        this.accumulator = new RecordAccumulator(batchSize, lingerMs, mappingManager);
//        this.sender = new Sender(session, this.accumulator);
//        this.cache = cache;
//        this.autoBatch = true;
//        Thread sendThread = new Thread(sender);
//        sendThread.start();
//        preIntStatement = session.prepare("insert into data_int (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
//        preLongStatement = session.prepare("insert into data_long (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
//        preFloatStatement = session.prepare("insert into data_float (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
//        preDoubleStatement = session.prepare("insert into data_double (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
//        preStringStatement = session.prepare("insert into data_text (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
//        preBooleanStatement = session.prepare("insert into data_boolean (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
//        preGeoStatement = session.prepare("insert into data_geo (host, metric, time_slice, primary_time, secondary_time, latitude, longitude) values (:host, :metric, :ts, :pt, :st, :la, :lo)");
//
//        preIntStatementWithoutST = session.prepare("insert into data_int (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
//        preLongStatementWithoutST = session.prepare("insert into data_long (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
//        preFloatStatementWithoutST = session.prepare("insert into data_float (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
//        preDoubleStatementWithoutST = session.prepare("insert into data_double (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
//        preStringStatementWithoutST = session.prepare("insert into data_text (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
//        preBooleanStatementWithoutST = session.prepare("insert into data_boolean (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
//        preGeoStatementWithoutST = session.prepare("insert into data_geo (host, metric, time_slice, primary_time, latitude, longitude) values (:host, :metric, :ts, :pt, :la, :lo)");
//
//        preLatestStatement = session.prepare("insert into latest (host, metric, time_slice) values (:host, :metric, :time_slice)");
//
//        preAggreStatement = session.prepare("insert into data_aggregation (host, metric, time_slice, max_value, min_value, count_value, sum_value) values (:host, :metric, :ts, :max, :min, :cnt, :sum)");
//
//        deleteIntStatement = session.prepare("delete from data_int where host = :h and metric = :m and time_slice = :t");
//        deleteLongStatement = session.prepare("delete from data_long where host = :h and metric = :m and time_slice = :t");
//        deleteFloatStatement = session.prepare("delete from data_float where host = :h and metric = :m and time_slice = :t");
//        deleteDoubleStatement = session.prepare("delete from data_double where host = :h and metric = :m and time_slice = :t");
//        deleteStringStatement = session.prepare("delete from data_text where host = :h and metric = :m and time_slice = :t");
//        deleteBooleanStatement = session.prepare("delete from data_boolean where host = :h and metric = :m and time_slice = :t");
//        deleteGeoStatement = session.prepare("delete from data_geo where host = :h and metric = :m and time_slice = :t");
//    }

    public SagittariusWriter(Session session, MappingManager mappingManager, Cache<HostMetricPair, TypePartitionPair> cache, int batchSize, int lingerMs, Monitor monitor) {
        this.session = session;
        this.mappingManager = mappingManager;
        this.accumulator = new RecordAccumulator(batchSize, lingerMs, mappingManager);
        this.sender = new Sender(session, this.accumulator, monitor);
        this.cache = cache;
        this.autoBatch = true;
        Thread sendThread = new Thread(sender);
        sendThread.start();
        preIntStatement = session.prepare("insert into data_int (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preLongStatement = session.prepare("insert into data_long (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preFloatStatement = session.prepare("insert into data_float (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preDoubleStatement = session.prepare("insert into data_double (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preStringStatement = session.prepare("insert into data_text (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
//        preBlobStatement = session.prepare("insert into data_blob (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preBooleanStatement = session.prepare("insert into data_boolean (host, metric, time_slice, primary_time, secondary_time, value) values (:host, :metric, :ts, :pt, :st, :v)");
        preGeoStatement = session.prepare("insert into data_geo (host, metric, time_slice, primary_time, secondary_time, latitude, longitude) values (:host, :metric, :ts, :pt, :st, :la, :lo)");

        preIntStatementWithoutST = session.prepare("insert into data_int (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preLongStatementWithoutST = session.prepare("insert into data_long (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preFloatStatementWithoutST = session.prepare("insert into data_float (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preDoubleStatementWithoutST = session.prepare("insert into data_double (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preStringStatementWithoutST = session.prepare("insert into data_text (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
//        preBlobStatementWithoutST = session.prepare("insert into data_blob (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preBooleanStatementWithoutST = session.prepare("insert into data_boolean (host, metric, time_slice, primary_time, value) values (:host, :metric, :ts, :pt, :v)");
        preGeoStatementWithoutST = session.prepare("insert into data_geo (host, metric, time_slice, primary_time, latitude, longitude) values (:host, :metric, :ts, :pt, :la, :lo)");

        preLatestStatement = session.prepare("insert into latest (host, metric, time_slice) values (:host, :metric, :time_slice)");

        preAggreStatement = session.prepare("insert into data_aggregation (host, metric, time_slice, max_value, min_value, count_value, sum_value) values (:host, :metric, :ts, :max, :min, :cnt, :sum)");

        deleteIntStatement = session.prepare("delete from data_int where host = :h and metric = :m and time_slice = :t");
        deleteLongStatement = session.prepare("delete from data_long where host = :h and metric = :m and time_slice = :t");
        deleteFloatStatement = session.prepare("delete from data_float where host = :h and metric = :m and time_slice = :t");
        deleteDoubleStatement = session.prepare("delete from data_double where host = :h and metric = :m and time_slice = :t");
        deleteStringStatement = session.prepare("delete from data_text where host = :h and metric = :m and time_slice = :t");
//        deleteBlobStatement = session.prepare("delete from data_boolean where host = :h and metric = :m and time_slice = :t");
        deleteBooleanStatement = session.prepare("delete from data_boolean where host = :h and metric = :m and time_slice = :t");
        deleteGeoStatement = session.prepare("delete from data_geo where host = :h and metric = :m and time_slice = :t");
    }

    @Override
    public void closeSender(){
        sender.close();
    }

    public Data newData() {
        return new Data();
    }

    public class Data {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        {
            batchStatement.setConsistencyLevel(ConsistencyLevel.ONE);
        }

        private void updateLatest(Latest candidate) {
//            HostMetricPair pair = new HostMetricPair(candidate.getHost(), candidate.getMetric());
//            if (latestData.containsKey(pair)) {
//                if (latestData.get(pair).getTimeSlice().compareTo(candidate.getTimeSlice()) < 0){
//                    latestData.put(pair, candidate);
//                    BoundStatement statement = preLatestStatement.bind(candidate.getHost(), candidate.getMetric(), candidate.getTimeSlice());
//                    batchStatement.add(statement);
//                }
//
//            } else {
//                latestData.put(pair, candidate);
//                BoundStatement statement = preLatestStatement.bind(candidate.getHost(), candidate.getMetric(), candidate.getTimeSlice());
//                batchStatement.add(statement);
//            }
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            BoundStatement statement = secondaryTime == -1? preIntStatementWithoutST.bind(host, metric, timeSlice, primaryTime, value)
                    : preIntStatement.bind(host, metric, timeSlice, primaryTime, secondaryTime, value);
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, int value) throws UnregisteredHostMetricException, DataTypeMismatchException {
            HostMetric hostMetric = getHostMetric(host, metric);
            if(hostMetric != null){
                if(hostMetric.getValueType() != ValueType.INT){
                    throw new DataTypeMismatchException("Mismatched DataType : Int. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
                }
                else {
                    addDatum(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
                }
            }
            else {
                throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
            }
        }

        public void addDatum(String host, String metric, String primaryTime, String secondaryTime, int value) throws UnregisteredHostMetricException, DataTypeMismatchException, ParseException {
            long pTime = TimeUtil.string2Date(primaryTime);
            long sTime = TimeUtil.string2Date(secondaryTime);
            addDatum(host, metric, pTime, sTime, value);
        }


        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            BoundStatement statement = secondaryTime == -1? preLongStatementWithoutST.bind(host, metric, timeSlice, primaryTime, value)
                    : preLongStatement.bind(host, metric, timeSlice, primaryTime, secondaryTime, value);
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, long value) throws UnregisteredHostMetricException, DataTypeMismatchException {
            HostMetric hostMetric = getHostMetric(host, metric);
            if(hostMetric != null){
                if(hostMetric.getValueType() != ValueType.LONG){
                    throw new DataTypeMismatchException("Mismatched DataType : Long. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
                }
                else {
                    addDatum(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
                }
            }
            else {
                throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
            }
        }

        public void addDatum(String host, String metric, String primaryTime, String secondaryTime, long value) throws UnregisteredHostMetricException, DataTypeMismatchException, ParseException {
            long pTime = TimeUtil.string2Date(primaryTime);
            long sTime = TimeUtil.string2Date(secondaryTime);
            addDatum(host, metric, pTime, sTime, value);
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            BoundStatement statement = secondaryTime == -1? preFloatStatementWithoutST.bind(host, metric, timeSlice, primaryTime, value)
                    : preFloatStatement.bind(host, metric, timeSlice, primaryTime, secondaryTime, value);
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, float value) throws UnregisteredHostMetricException, DataTypeMismatchException {
            HostMetric hostMetric = getHostMetric(host, metric);
            if(hostMetric != null){
                if(hostMetric.getValueType() != ValueType.FLOAT){
                    throw new DataTypeMismatchException("Mismatched DataType : Float. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
                }
                else {
                    addDatum(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
                }
            }
            else {
                throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
            }
        }

        public void addDatum(String host, String metric, String primaryTime, String secondaryTime, float value) throws UnregisteredHostMetricException, DataTypeMismatchException, ParseException {
            long pTime = TimeUtil.string2Date(primaryTime);
            long sTime = TimeUtil.string2Date(secondaryTime);
            addDatum(host, metric, pTime, sTime, value);
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            BoundStatement statement = secondaryTime == -1? preDoubleStatementWithoutST.bind(host, metric, timeSlice, primaryTime, value)
                    : preDoubleStatement.bind(host, metric, timeSlice, primaryTime, secondaryTime, value);
            batchStatement.add(statement);
//            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, double value) throws UnregisteredHostMetricException, DataTypeMismatchException {
            HostMetric hostMetric = getHostMetric(host, metric);
            if(hostMetric != null){
                if(hostMetric.getValueType() != ValueType.DOUBLE){
                    throw new DataTypeMismatchException("Mismatched DataType : Double. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
                }
                else {
                    addDatum(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
                }
            }
            else {
                throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
            }
        }

        public void addDatum(String host, String metric, String primaryTime, String secondaryTime, double value) throws UnregisteredHostMetricException, DataTypeMismatchException, ParseException {
            long pTime = TimeUtil.string2Date(primaryTime);
            long sTime = TimeUtil.string2Date(secondaryTime);
            addDatum(host, metric, pTime, sTime, value);
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            BoundStatement statement = secondaryTime == -1? preBooleanStatementWithoutST.bind(host, metric, timeSlice, primaryTime, value)
                    : preBooleanStatement.bind(host, metric, timeSlice, primaryTime, secondaryTime, value);
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, boolean value) throws UnregisteredHostMetricException, DataTypeMismatchException {
            HostMetric hostMetric = getHostMetric(host, metric);
            if(hostMetric != null){
                if(hostMetric.getValueType() != ValueType.BOOLEAN){
                    throw new DataTypeMismatchException("Mismatched DataType : Boolean. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
                }
                else {
                    addDatum(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
                }
            }
            else {
                throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
            }
        }

        public void addDatum(String host, String metric, String primaryTime, String secondaryTime, boolean value) throws UnregisteredHostMetricException, DataTypeMismatchException, ParseException {
            long pTime = TimeUtil.string2Date(primaryTime);
            long sTime = TimeUtil.string2Date(secondaryTime);
            addDatum(host, metric, pTime, sTime, value);
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, ByteBuffer value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            BoundStatement statement = secondaryTime == -1? preBlobStatementWithoutST.bind(host, metric, timeSlice, primaryTime, value)
                    : preBlobStatement.bind(host, metric, timeSlice, primaryTime, secondaryTime, value);
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, ByteBuffer value) throws UnregisteredHostMetricException, DataTypeMismatchException {
            HostMetric hostMetric = getHostMetric(host, metric);
            if(hostMetric != null){
                if(hostMetric.getValueType() != ValueType.BLOB){
                    throw new DataTypeMismatchException("Mismatched DataType : Blob. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
                }
                else {
                    addDatum(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
                }
            }
            else {
                throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
            }
        }

        public void addDatum(String host, String metric, String primaryTime, String secondaryTime, ByteBuffer value) throws UnregisteredHostMetricException, DataTypeMismatchException, ParseException {
            long pTime = TimeUtil.string2Date(primaryTime);
            long sTime = TimeUtil.string2Date(secondaryTime);
            addDatum(host, metric, pTime, sTime, value);
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            BoundStatement statement = secondaryTime == -1? preStringStatementWithoutST.bind(host, metric, timeSlice, primaryTime, value)
                    : preStringStatement.bind(host, metric, timeSlice, primaryTime, secondaryTime, value);
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, String value) throws UnregisteredHostMetricException, DataTypeMismatchException {
            HostMetric hostMetric = getHostMetric(host, metric);
            if(hostMetric != null){
                if(hostMetric.getValueType() != ValueType.STRING){
                    throw new DataTypeMismatchException("Mismatched DataType : String. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
                }
                else {
                    addDatum(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
                }
            }
            else {
                throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
            }
        }

        public void addDatum(String host, String metric, String primaryTime, String secondaryTime, String value) throws UnregisteredHostMetricException, DataTypeMismatchException, ParseException {
            long pTime = TimeUtil.string2Date(primaryTime);
            long sTime = TimeUtil.string2Date(secondaryTime);
            addDatum(host, metric, pTime, sTime, value);
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) {
            String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
            BoundStatement statement = secondaryTime == -1? preGeoStatementWithoutST.bind(host, metric, timeSlice, primaryTime, latitude, longitude)
                    : preGeoStatement.bind(host, metric, timeSlice, primaryTime, secondaryTime, latitude, longitude);
            batchStatement.add(statement);
            updateLatest(new Latest(host, metric, timeSlice));
        }

        public void addDatum(String host, String metric, long primaryTime, long secondaryTime,  float latitude, float longitude) throws UnregisteredHostMetricException, DataTypeMismatchException {
            HostMetric hostMetric = getHostMetric(host, metric);
            if(hostMetric != null){
                if(hostMetric.getValueType() != ValueType.GEO){
                    throw new DataTypeMismatchException("Mismatched DataType : Geo. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
                }
                else {
                    addDatum(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), latitude, longitude);
                }
            }
            else {
                throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
            }
        }

        public void addDatum(String host, String metric, String primaryTime, String secondaryTime, float lagitude, float longitude) throws UnregisteredHostMetricException, DataTypeMismatchException, ParseException {
            long pTime = TimeUtil.string2Date(primaryTime);
            long sTime = TimeUtil.string2Date(secondaryTime);
            addDatum(host, metric, pTime, sTime, lagitude, longitude);
        }
    }

    @Override
    public void registerHostMetricInfo(String host, List<MetricMetadata> metricMetadatas) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            Mapper<HostMetric> mapper = mappingManager.mapper(HostMetric.class);
            BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (MetricMetadata metricMetadata : metricMetadatas) {
                Statement statement = mapper.saveQuery(new HostMetric(host, metricMetadata.getMetric(), metricMetadata.getTimePartition(), metricMetadata.getValueType(), metricMetadata.getDescription()), saveNullFields(false));
                batchStatement.add(statement);
            }
            session.execute(batchStatement);
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void registerHostTags(String host, Map<String, String> tags) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            Mapper<HostTags> mapper = mappingManager.mapper(HostTags.class);
            mapper.save(new HostTags(host, tags), saveNullFields(false));
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, int value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                //secondaryTime use boxed type so it can be set to null and won't be store in cassandra.
                //see com.datastax.driver.mapping.Mapper : saveNullFields
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<IntData> dataMapper = mappingManager.mapper(IntData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new IntData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, int value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException, UnregisteredHostMetricException, DataTypeMismatchException {
        HostMetric hostMetric = getHostMetric(host, metric);
        if(hostMetric != null){
            if(hostMetric.getValueType() != ValueType.INT){
                throw new DataTypeMismatchException("Mismatched DataType : Int. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
            }
            else {
                insert(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
            }
        }
        else {
            throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, long value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<LongData> dataMapper = mappingManager.mapper(LongData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new LongData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, long value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException, UnregisteredHostMetricException, DataTypeMismatchException {
        HostMetric hostMetric = getHostMetric(host, metric);
        if(hostMetric != null){
            if(hostMetric.getValueType() != ValueType.LONG){
                throw new DataTypeMismatchException("Mismatched DataType : Long. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
            }
            else {
                insert(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
            }
        }
        else {
            throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<FloatData> dataMapper = mappingManager.mapper(FloatData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new FloatData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, float value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException, UnregisteredHostMetricException, DataTypeMismatchException {
        HostMetric hostMetric = getHostMetric(host, metric);
        if(hostMetric != null){
            if(hostMetric.getValueType() != ValueType.FLOAT){
                throw new DataTypeMismatchException("Mismatched DataType : Float. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
            }
            else {
                insert(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
            }
        }
        else {
            throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, double value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<DoubleData> dataMapper = mappingManager.mapper(DoubleData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new DoubleData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, double value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException, UnregisteredHostMetricException, DataTypeMismatchException {
        HostMetric hostMetric = getHostMetric(host, metric);
        if(hostMetric != null){
            if(hostMetric.getValueType() != ValueType.DOUBLE){
                throw new DataTypeMismatchException("Mismatched DataType : Double. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
            }
            else {
                insert(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
            }
        }
        else {
            throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, boolean value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<BooleanData> dataMapper = mappingManager.mapper(BooleanData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new BooleanData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, boolean value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException, UnregisteredHostMetricException, DataTypeMismatchException {
        HostMetric hostMetric = getHostMetric(host, metric);
        if(hostMetric != null){
            if(hostMetric.getValueType() != ValueType.BOOLEAN){
                throw new DataTypeMismatchException("Mismatched DataType : Boolean. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
            }
            else {
                insert(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
            }
        }
        else {
            throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, String value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, value);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<StringData> dataMapper = mappingManager.mapper(StringData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new StringData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, value), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, String value) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException, UnregisteredHostMetricException, DataTypeMismatchException {
        HostMetric hostMetric = getHostMetric(host, metric);
        if(hostMetric != null){
            if(hostMetric.getValueType() != ValueType.STRING){
                throw new DataTypeMismatchException("Mismatched DataType : String. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
            }
            else {
                insert(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), value);
            }
        }
        else {
            throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, TimePartition timePartition, float latitude, float longitude) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            if (autoBatch) {
                accumulator.append(host, metric, primaryTime, secondaryTime, timePartition, latitude, longitude);
            } else {
                String timeSlice = TimeUtil.generateTimeSlice(primaryTime, timePartition);
                Long boxedSecondaryTime = secondaryTime == -1 ? null : secondaryTime;
                Mapper<GeoData> dataMapper = mappingManager.mapper(GeoData.class);
                Mapper<Latest> latestMapper = mappingManager.mapper(Latest.class);
                dataMapper.save(new GeoData(host, metric, timeSlice, primaryTime, boxedSecondaryTime, latitude, longitude), saveNullFields(false));
                latestMapper.save(new Latest(host, metric, timeSlice), saveNullFields(false));
            }
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void insert(String host, String metric, long primaryTime, long secondaryTime, float latitude, float longitude) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException, UnregisteredHostMetricException, DataTypeMismatchException {
        HostMetric hostMetric = getHostMetric(host, metric);
        if(hostMetric != null){
            if(hostMetric.getValueType() != ValueType.GEO){
                throw new DataTypeMismatchException("Mismatched DataType : Geo. DataType of the value should be " + getValueTypeString(hostMetric.getValueType()));
            }
            else {
                insert(host, metric, primaryTime, secondaryTime, hostMetric.getTimePartition(), latitude, longitude);
            }
        }
        else {
            throw new UnregisteredHostMetricException("Unregistered hostMetric : " + host + " " + metric);
        }
    }

    @Override
    public void insert(String host, String metric, long timeSlice, double maxValue, double minValue, double countValue, double sumValue) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
            Mapper<AggregationData> dataMapper = mappingManager.mapper(AggregationData.class);
            dataMapper.save(new AggregationData(host, metric, timeSlice, maxValue, minValue, countValue, sumValue), saveNullFields(false));
        }catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
    }

    public void bulkInsert(Data data) throws com.sagittarius.exceptions.NoHostAvailableException, TimeoutException, com.sagittarius.exceptions.QueryExecutionException {
        try {
//            for (Map.Entry<HostMetricPair, Latest> entry : latestData.entrySet()) {
//                BoundStatement statement = preLatestStatement.bind(entry.getValue().getHost(), entry.getValue().getMetric(), entry.getValue().getTimeSlice());
//                data.batchStatement.add(statement);
//            }
            session.execute(data.batchStatement);
        } catch (NoHostAvailableException e) {
            throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
        } catch (OperationTimedOutException | WriteTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e.getCause());
        } catch (QueryExecutionException e) {
            throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
        }
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

    private String generateInStatement(Collection<String> params) {
        StringBuilder sb = new StringBuilder();
        for (String param : params) {
            sb.append("'").append(param).append("'").append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public HostMetric getHostMetric(String host, String metric) {
        List<HostMetric> result = new ArrayList<>();
        List<String> queryHosts = new ArrayList<>(), queryMetrics = new ArrayList<>();
        //first visit cache, if do not exist in cache, then query cassandra
        TypePartitionPair typePartition = cache.get(new HostMetricPair(host, metric));
        if (typePartition != null) {
            return (new HostMetric(host, metric, typePartition.getTimePartition(), typePartition.getValueType(), null));
        } else {
            queryHosts.add(host);
            queryMetrics.add(metric);
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

        return result.size() > 0 ? result.get(0) : null;
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

    private List<String> getTimeSlices(long startTime, long endTime, TimePartition timePartition){
        ArrayList<String> timeSlices = new ArrayList<>();

        LocalDateTime start = LocalDateTime.ofEpochSecond(startTime/1000, 0, TimeUtil.zoneOffset);
        LocalDateTime end = LocalDateTime.ofEpochSecond(endTime/1000, 0, TimeUtil.zoneOffset);
        switch (timePartition){
            case DAY:{

                while (start.isBefore(end)){
                    timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * 1000, TimePartition.DAY));
                    start = start.plusDays(1);
                }
                break;
            }
            case WEEK:{
                while (start.isBefore(end)){
                    timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * 1000, TimePartition.WEEK));
                    start = start.plusWeeks(1);
                }
                break;
            }
            case MONTH:{
                while (start.isBefore(end)){
                    timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * 1000, TimePartition.MONTH));
                    start= start.plusMonths(1);
                }
                break;
            }
            case YEAR:{
                while (start.isBefore(end)){
                    timeSlices.add(TimeUtil.generateTimeSlice(start.toEpochSecond(TimeUtil.zoneOffset) * 1000, TimePartition.YEAR));
                    start = start.plusYears(1);
                }
                break;
            }
        }
        return timeSlices;
    }

    @Override
    public void deleteRange(List<String> hosts, List<String> metrics, long startTime, long endTime) throws com.sagittarius.exceptions.QueryExecutionException, TimeoutException, com.sagittarius.exceptions.NoHostAvailableException {
        BatchStatement batchStatement = new BatchStatement();
        List<HostMetric> hostMetrics = getHostMetrics(hosts, metrics);
        for(HostMetric hostMetric : hostMetrics){
            String host = hostMetric.getHost();
            String metric = hostMetric.getMetric();
            TimePartition timePartition = hostMetric.getTimePartition();
            ValueType valueType = hostMetric.getValueType();
            PreparedStatement deleteStatement = null;
            switch (valueType){
                case INT:
                    deleteStatement = deleteIntStatement;
                    break;
                case LONG:
                    deleteStatement = deleteLongStatement;
                    break;
                case FLOAT:
                    deleteStatement = deleteFloatStatement;
                    break;
                case DOUBLE:
                    deleteStatement = deleteDoubleStatement;
                    break;
                case STRING:
                    deleteStatement = deleteStringStatement;
                    break;
                case BOOLEAN:
                    deleteStatement = deleteBooleanStatement;
                    break;
                case GEO:
                    deleteStatement = deleteGeoStatement;
                    break;
            }
            if(deleteStatement == null){
                continue;
            }
            List<String> timeSlices = getTimeSlices(startTime, endTime, timePartition);
            for (String timeSlice : timeSlices){
                BoundStatement statement = deleteStatement.bind(host, metric, timeSlice);
                batchStatement.add(statement);
            }
            try{
                session.execute(batchStatement);
            } catch (NoHostAvailableException e) {
                throw new com.sagittarius.exceptions.NoHostAvailableException(e.getMessage(), e.getCause());
            } catch (OperationTimedOutException | ReadTimeoutException e) {
                throw new TimeoutException(e.getMessage(), e.getCause());
            } catch (QueryExecutionException e) {
                throw new com.sagittarius.exceptions.QueryExecutionException(e.getMessage(), e.getCause());
            }

            batchStatement.clear();
        }
    }

    private String getValueTypeString(ValueType valueType){
        if(valueType == ValueType.INT){
            return "Int";
        }
        if(valueType == ValueType.LONG){
            return "Long";
        }
        if(valueType == ValueType.FLOAT){
            return "Float";
        }
        if(valueType == ValueType.DOUBLE){
            return "Double";
        }
        if(valueType == ValueType.STRING){
            return "String";
        }
        if(valueType == ValueType.BOOLEAN){
            return "Boolean";
        }
        if(valueType == ValueType.GEO){
            return "Geo";
        }

        return "";
    }
}
