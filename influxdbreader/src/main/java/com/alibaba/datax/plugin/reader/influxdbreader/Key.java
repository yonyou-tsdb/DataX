package com.alibaba.datax.plugin.reader.influxdbreader;

/**
 * Function: Key
 * @author Shenguanchu
 * @since 2022-09-21
 */
public class Key {
    public static final String CONNECTION = "connection";
    public static final String URL = "url";
    public static final String TOKEN = "token";
    public static final String ORG = "org";
    public static final String BUCKET = "bucket";
    public static final String DATABASE = "database";
    public static final String BEGIN_DATETIME = "beginDateTime";
    public static final String END_DATETIME = "endDateTime";
    public static final String INTERVAL_DATE_TIME = "splitIntervalHour";
    public static final Integer INTERVAL_DATE_TIME_DEFAULT_VALUE = 240;
    public static final String MINI_TASK_INTERVAL_SECOND = "miniTaskIntervalSecond";
    public static final String READE_TIMEOUT = "readTimeout";
    public static final String WRITE_TIMEOUT = "writeTimeout";
    public static final String CONNECT_TIMEOUT = "connectTimeout";
    public static final String INFLUXDB_VERSION = "influxdbVersion";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String RETENTION_POLICY = "retentionPolicy";

}
