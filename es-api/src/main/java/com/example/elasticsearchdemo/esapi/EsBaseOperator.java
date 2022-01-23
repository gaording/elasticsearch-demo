package com.example.elasticsearchdemo.esapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @program: elasticsearch-demo
 * @date: 2022/1/21
 * @author: gaorunding1
 * @description:
 **/
public interface EsBaseOperator<T> {

    /**
     * 初始化日志
     */
    Logger log = LoggerFactory.getLogger(EsIndexOperator.class);

    /**
     * 默认type
     */
    String DEFAULT_TYPE="_doc";

    /**
     * 初始化客户端
     */
    void initClient();

    /**
     * 关闭客户端
     */
    void close();

    /**
     * 获取es客户端
     * @return
     */
    T getClient();
}
