package com.sagittarius.util;

import com.sagittarius.bean.common.TimePartition;

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
    public static final ZoneOffset zoneOffset = ZoneOffset.ofHours(8);
    public static final SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public static String generateTimeSlice(long timeMillis, TimePartition timePartition) {
        LocalDateTime time = LocalDateTime.ofEpochSecond(timeMillis/1000, 0, zoneOffset);
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

    public static String date2String(long date, SimpleDateFormat sdf) {
        return sdf.format(new Date(date));
    }
    public static String date2String(long time) {
        if(time < 0) return "";
        return LocalDateTime.ofEpochSecond(time/1000, (int)(time%1000*1000), zoneOffset).toString();
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
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        }
        long primaryTime = java.sql.Timestamp.valueOf(LocalDateTime.parse(time, formatter)).getTime();
        return primaryTime;
    }
}
