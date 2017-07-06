package com.sagittarius.example;

import jline.console.completer.Completer;

import java.util.List;

/**
 * Created by hadoop on 17-4-17.
 */
public class MyCompleter implements Completer {
    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
    {
        if(buffer.length() > 0 && cursor >0)
        {
            String input = buffer.substring(0,cursor);
            String[] substrings = input.split(" ");
            String substring = substrings[substrings.length-1].toLowerCase();
            int substart = cursor - substring.length();
            if(substring.startsWith("getr")){
                candidates.add("getRange");
                return substart;
            }
            if(substring.startsWith("getp")){
                candidates.add("getPoint");
                return substart;
            }
            if(substring.startsWith("getintp"))
            {
                candidates.add("getIntPoint");
                return substart;
            }
            if(substring.startsWith("getintr"))
            {
                candidates.add("getIntRange");
                return substart;
            }
            if(substring.startsWith("geti"))
            {
                candidates.add("getIntPoint");
                candidates.add("getIntRange");
                return substart;
            }
            if(substring.startsWith("getlongp"))
            {
                candidates.add("getLongPoint");
                return substart;
            }
            if(substring.startsWith("getlongr"))
            {
                candidates.add("getLongRange");
                return substart;
            }
            if(substring.startsWith("getl"))
            {
                candidates.add("getLongPoint");
                candidates.add("getLongRange");
                return substart;
            }
            if(substring.startsWith("getfloatp"))
            {
                candidates.add("getFloatPoint");
                return substart;
            }
            if(substring.startsWith("getfloatr"))
            {
                candidates.add("getFloatRange");
                return substart;
            }
            if(substring.startsWith("getf"))
            {
                candidates.add("getFloatPoint");
                candidates.add("getFloatRange");
                return substart;
            }
            if(substring.startsWith("getdoublep"))
            {
                candidates.add("getDoublePoint");
                return substart;
            }
            if(substring.startsWith("getdoubler"))
            {
                candidates.add("getDoubleRange");
                return substart;
            }
            if(substring.startsWith("getd"))
            {
                candidates.add("getDoublePoint");
                candidates.add("getDoubleRange");
                return substart;
            }
            if(substring.startsWith("getbooleanp"))
            {
                candidates.add("getBooleanPoint");
                return substart;
            }
            if(substring.startsWith("getbooleanr"))
            {
                candidates.add("getBooleanRange");
                return substart;
            }
            if(substring.startsWith("getb"))
            {
                candidates.add("getBooleanPoint");
                candidates.add("getBooleanRange");
                return substart;
            }
            if(substring.startsWith("getstringp"))
            {
                candidates.add("getStringPoint");
                return substart;
            }
            if(substring.startsWith("getstringr"))
            {
                candidates.add("getStringRange");
                return substart;
            }
            if(substring.startsWith("gets"))
            {
                candidates.add("getStringPoint");
                candidates.add("getStringRange");
                return substart;
            }
            if(substring.startsWith("getgeop"))
            {
                candidates.add("getGeoPoint");
                return substart;
            }
            if(substring.startsWith("getgeor"))
            {
                candidates.add("getGeoRange");
                return substart;
            }
            if(substring.startsWith("gets"))
            {
                candidates.add("getGeoPoint");
                candidates.add("getGeoRange");
                return substart;
            }
            if(substring.startsWith("geti"))
            {
                candidates.add("getIntPoint");
                return substart;
            }
            if(substring.startsWith("getl"))
            {
                candidates.add("getLongPoint");
                return substart;
            }
            if(substring.startsWith("getf"))
            {
                candidates.add("getFloatPoint");
                return substart;
            }
            if(substring.startsWith("getd"))
            {
                candidates.add("getDoublePoint");
                return substart;
            }
            if(substring.startsWith("getb"))
            {
                candidates.add("getBooleanPoint");
                return substart;
            }
            if(substring.startsWith("gets"))
            {
                candidates.add("getStringPoint");
                return substart;
            }
            if(substring.startsWith("getg"))
            {
                candidates.add("getGeoPoint");
                return substart;
            }
            if(substring.startsWith("g"))
            {
                candidates.add("getSensorsByDeviceId");
                candidates.add("getIntPoint");
                candidates.add("getLongPoint");
                candidates.add("getFloatPoint");
                candidates.add("getDoublePoint");
                candidates.add("getBooleanPoint");
                candidates.add("getStringPoint");
                candidates.add("getGeoPoint");
                candidates.add("getIntRange");
                candidates.add("getLongRange");
                candidates.add("getFloatRange");
                candidates.add("getDoubleRange");
                candidates.add("getBooleanRange");
                candidates.add("getStringRange");
                candidates.add("getGeoRange");
                return substart;
            }
            if(substring.startsWith("i"))
            {
                candidates.add("insert");
                return substart;
            }
            if(substring.startsWith("d"))
            {
                candidates.add("date2long");
                return substart;
            }
        }
        return cursor;
    }
}
