package com.yolo.estemplate.controller;


import org.joda.time.DateTime;
import org.joda.time.Days;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTest {
    public static void main(String[] args) throws ParseException {
        String endTime = new String("2021-05-31 10:22:22");
        String beginTime = new String("2021-03-01 11:22:22");

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = df.parse(beginTime);
        Date end = df.parse(endTime);

        DateTime starttime = new DateTime(start);
        DateTime endtieme = new DateTime(end);
        Days days = Days.daysBetween(starttime, endtieme);
        System.out.println(days.getDays());
        Long taskId = 1L;
    }
}
