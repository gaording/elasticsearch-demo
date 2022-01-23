package com.example.elasticsearchdemo.esapi;

import java.util.List;
import java.util.Map;

/**
 * @program: elasticsearch-demo
 * @date: 2022/1/19
 * @author: gaorunding1
 * @description: es查询操作类(get、search、page、avg等)
 **/
public interface EsQueryOperator{
    /**
     * 根据docId查询doc（es是根据docId进行哈希定位到不同的分片上存储，get比search快很多）
     * @param indexName
     * @param docId
     */
    String getDoc(String indexName,String docId);

    /**
     * 根据docIds查询多个doc
     * @param indexName
     * @param docIds
     */
    List<String> mulitGet(String indexName, String... docIds);

    /**
     * 自己构建条件json进行search
     * 大部分情况下，各种条件主要是对queryJson的构建，详情查看es官网中各种query dsl使用
     * 比如bool（must、must_not、should、filter）、term、terms、exists、ids、range、match、regexp、query_string等等
     * @param indexName
     * @return
     */
    List<String> search(String indexName, String queryJson);

    /**
     * 通过from、size进行分页查询
     * es默认from+size不能超过1w，原因是如果有n个shard，那么要排前1w数据时，es会先从n个shard中分别取出1w数据（具体搜索查看queryAndFetch过程），
     * 所以总共取出n*1w条数据汇合到协调节点再次进行排序，如果请求的from+size过多，会比较消耗es性能，所以es服务端默认判定from+size超过1w时会报错，
     * （当然es也有配置项可更改1w的配置，但建议超过1w的数据就不要用from、size进行分页了）
     *  @param indexName
     * @param pageNum
     * @param pageSize
     * @return
     */
    List<String> searchPageByFromSize(String indexName, String queryJson, int pageNum, int pageSize);

    /**
     * 通过scroll先在es服务端构建一个快照，生成符合当前请求条件的返回结果。
     * scroll可以配置这个快照能留存多久，比如留存5min，那么每次请求时会再次刷新快照留存时间为5min。
     * 有两个问题：1。5min内的增删改doc都不会在快照中看到，不是实时结果  2。如果scroll请求过多，es服务端会被占用大量内存，影响服务端使用
     * 适用场景：大量导出数据的情况，比如数据备份、或者批量导入到spark、flink处理等
     * @param indexName
     * @param queryJson
     * @param scrollMinute
     * @param pageNum
     * @param pageSize
     */
    List<String> searchPageByScroll(String indexName, String queryJson, int scrollMinute, int pageNum, int pageSize);

    /**
     * 通过searchAfter走queryAndFetch流程，和from=1w+size=10不一样的地方在于，第一步query的时候不是返回n*(1w+10)条数据，而是每个shard
     * 先根据sortValues进行排序，获取排序后的size个docId，然后协调节点在fetch阶段只会拿到n*size个数据。
     * 相当于每次仍要排序，但是最后fetch阶段聚合结果的量少了很多。而且是返回实时的结果。7.0后为官方推荐深度分页用法
     *
     *
     * searchAfter分页重点在于要提供的sortValues，es每个shard会获取满足sortValues（降序或升序）之后的pageSize数据，然后在协调节点fetch排序
     *
     * @param indexName
     * @param queryJson
     * @param pageNum
     * @param pageSize
     */
    List<String> searchPageBySearchAfter(String indexName, String queryJson, int pageNum, int pageSize);

    /**
     * 对返回结果进行排序
     * @param indexName
     * @param queryJson
     * @param sortFields
     */
    List<String> searchWithSort(String indexName, String queryJson, String... sortFields);

    /**
     * es将aggregation分为三类，一类是metric（指标类信息，count、sum、avg、中位数等），一类是bulk（分组统计，就是group by语句），
     * 还有一类是pipeline（group by之后的操作，我大概理解为mysql中的having吧）
     *
     * 获得聚合统计结果等
     * 比如sum、count等,这里只以sum为例，别的metricAggregation操作类似。详细可查看官网相关信息
     * @param indexName
     * @param sumField
     */
    Double searchWithMetricAggregation(String indexName, String sumField);

    /**
     * 获得bulk分组统计结果，等同于mysql的groupby
     *
     * @param indexName
     * @param groupField
     * @return
     */
    Map<String,Long> searchWithBulkAggregation(String indexName, String groupField);

    /**
     * 先groupby再对groupby之后的bulk进行aggregation操作
     * 6.x这种操作叫做structuring_aggregations
     *
     * @param indexName
     * @param groupField
     * @param sumField
     * @return
     */
    Map<String, Double> searchWithPipelineAggregation(String indexName, String groupField, String sumField);

    /**
     * 一次搜索多个索引
     * @param indexNames
     * @return
     */
    List<String> multiSearchWithIndexNames(String... indexNames);

    /**
     * 一次搜索多个条件
     * @param queryJson
     * @return
     */
    List<List<String>> multiSearchWithQueryJsons(String indexName, String... queryJson);


    void asyncSearch(String indexName);

}
