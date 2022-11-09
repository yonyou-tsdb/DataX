package com.alibaba.datax.plugin.reader.influxdbreader;

public final class Constant {
    static final String DEFAULT_DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";
    static final int DEFAULT_HOUR_TO_MILLISECOND = 60 * 60 * 1000;
    static final int DEFAULT_READ_TIMEOUT_SECOND = 300;
    static final int DEFAULT_WRITE_TIMEOUT_SECOND = 300;
    static final int DEFAULT_CONNECT_TIMEOUT_SECOND = 60;
    static final String DEFAULT_TASK_ID_LOG_RELATIVE_PATH = "\\log\\taskId.log";
    static final String DATAX_HOME = "datax.home";
    /** read timeout default first time interval: 20s */
    static final int READ_TIMEOUT_RETRY_TIME_INTERVAL = 20;
    static final int READ_TIMEOUT_RETRY_NUM = 3;
}
