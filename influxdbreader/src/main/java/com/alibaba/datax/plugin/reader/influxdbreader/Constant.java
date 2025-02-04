package com.alibaba.datax.plugin.reader.influxdbreader;

public final class Constant {
    static final String DEFAULT_DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";
    static final int DEFAULT_HOUR_TO_MILLISECOND = 60 * 60 * 1000;
    static final int DEFAULT_READ_TIMEOUT_SECOND = 60;
    static final int DEFAULT_WRITE_TIMEOUT_SECOND = 60;
    static final int DEFAULT_CONNECT_TIMEOUT_SECOND = 60;
    static final int DEFAULT_QUERY_LIMIT = 1000;
    static final String DEFAULT_TASK_ID_LOG_RELATIVE_PATH = "\\log\\taskId.log";
    static final String DATAX_HOME = "datax.home";
}
