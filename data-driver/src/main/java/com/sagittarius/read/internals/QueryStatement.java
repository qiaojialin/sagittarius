package com.sagittarius.read.internals;

public class QueryStatement {
    public static final String HOST_METRICS_QUERY_STATEMENT = "select * from host_metric where host in (%s) and metric in (%s)";
    public static final String POINT_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s' and primary_time=%d";
    public static final String POINT_BEFORE_SHIFT_QUERY_STATEMENT = "select * from %s where host='%s' and metric='%s' and time_slice='%s' and primary_time<=%d order by metric DESC, primary_time DESC limit 1 ";
    public static final String POINT_AFTER_SHIFT_QUERY_STATEMENT = "select * from %s where host='%s' and metric='%s' and time_slice='%s' and primary_time>=%d order by metric DESC, primary_time limit 1";
    public static final String LATEST_TIMESLICE_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s)";
    public static final String LATEST_POINT_QUERY_STATEMENT = "select * from %s where host='%s' and metric='%s' and time_slice='%s' order by metric DESC, primary_time DESC limit 1 ";

    public static final String WHOLE_PARTITION_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s'";
    public static final String PARTIAL_PARTITION_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s' and primary_time%s%d";
    public static final String IN_PARTITION_QUERY_STATEMENT = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s' and primary_time>=%d and primary_time<=%d";

    public static final String WHOLE_PARTITION_QUERY_STATEMENT_DESC = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s' order by metric DESC, primary_time DESC";
    public static final String PARTIAL_PARTITION_QUERY_STATEMENT_DESC = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s' and primary_time%s%d order by metric DESC, primary_time DESC";
    public static final String IN_PARTITION_QUERY_STATEMENT_DESC = "select * from %s where host in (%s) and metric in (%s) and time_slice='%s' and primary_time>=%d and primary_time<=%d order by metric DESC, primary_time DESC";

    public static final String WHOLE_PARTITION_WHERE_STATEMENT = "host in (%s) and metric in (%s) and time_slice='%s' %s allow filtering";
    public static final String PARTIAL_PARTITION_WHERE_STATEMENT = "host in (%s) and metric in (%s) and time_slice='%s' and primary_time%s%d %s allow filtering";
    public static final String IN_PARTITION_WHERE_STATEMENT = "host in (%s) and metric in (%s) and time_slice='%s' and primary_time>=%d and primary_time<=%d %s allow filtering";

    public static final String WHOLE_PARTITION_WHERE_STATEMENT_DESC = "host in (%s) and metric in (%s) and time_slice='%s' %s order by metric DESC, primary_time DESC  allow filtering";
    public static final String PARTIAL_PARTITION_WHERE_STATEMENT_DESC = "host in (%s) and metric in (%s) and time_slice='%s' and primary_time%s%d %s order by metric DESC, primary_time DESC allow filtering";
    public static final String IN_PARTITION_WHERE_STATEMENT_DESC = "host in (%s) and metric in (%s) and time_slice='%s' and primary_time>=%d and primary_time<=%d %s order by metric DESC, primary_time DESC allow filtering";

    public static final String WHOLE_PARTITION_WHERE_STATEMENT_AGG = "host = '%s' and metric = '%s' and time_slice in (%s) %s ";
    public static final String PARTIAL_PARTITION_WHERE_STATEMENT_AGG = "host = '%s' and metric = '%s' and time_slice in (%s) and primary_time%s%d %s ";
    public static final String IN_PARTITION_WHERE_STATEMENT_AGG = "host = '%s' and metric = '%s' and time_slice in (%s) and primary_time>=%d and primary_time<=%d %s ";

}
