package com.sagittarius.util;

import com.sagittarius.bean.common.TimePartition;
import jnr.ffi.Struct;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.sql.Timestamp;

import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;

public class TimeUtil {
    private static long SECOND_TO_MICROSECOND = 1000L;
    public static final ZoneOffset zoneOffset = ZoneOffset.ofHours(8);
    public static final SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public static String generateTimeSlice(long timeMillis, TimePartition timePartition) {
        LocalDateTime time = LocalDateTime.ofEpochSecond(timeMillis/SECOND_TO_MICROSECOND, 0, zoneOffset);
        switch (timePartition) {
            case DAY:
                return time.getYear() + "D" + time.getDayOfYear();
            case WEEK:
                return time.getYear() + "W" + time.get(ALIGNED_WEEK_OF_YEAR);
            case MONTH:
                return time.getYear() + "M" + time.getMonthValue();
            case YEAR:
                return time.getYear() + "";
            default:
                return null;
        }
    }

    public static String generateTimeSlice2(long timeMillis, TimePartition timePartition) {
        LocalDateTime time = LocalDateTime.ofEpochSecond(timeMillis/SECOND_TO_MICROSECOND, 0, zoneOffset);
        switch (timePartition) {
            case DAY:
                return time.getYear() + "D" + String.format("%03d", time.getDayOfYear());
            case WEEK:
                return time.getYear() + "W" + String.format("%03d", time.get(ALIGNED_WEEK_OF_YEAR));
            case MONTH:
                return time.getYear() + "M" + String.format("%03d", time.getMonthValue());
            case YEAR:
                return time.getYear() + "";
            default:
                return null;
        }
    }

//    public static String date2String(long date, SimpleDateFormat sdf) {
//        return sdf.format(new Date(date));
//    }
    public static String date2String(long time) {
        if(time < 0) return "";
        return LocalDateTime.ofEpochSecond(time/SECOND_TO_MICROSECOND, (int)(time%SECOND_TO_MICROSECOND*1000), zoneOffset).toString();
    }

//    public static long string2Date(String time, SimpleDateFormat sdf) throws ParseException, NumberFormatException {
//        long result;
//        try{
//            result = sdf.parse(time).getTime();
//        } catch (Exception e){
//            result = -1;
//        }
//
//        return result;
//    }

    public static long string2Date(String time) throws ParseException, NumberFormatException {
        if(time == null){
            return -1L;
        }
        DateTimeFormatter formatter;
        if(time.length() == "yyyy-MM-dd HH:mm:ss".length()){
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        }
        else{
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        }
        long primaryTime = java.sql.Timestamp.valueOf(LocalDateTime.parse(time, formatter)).getTime();
        return primaryTime;
    }
}
