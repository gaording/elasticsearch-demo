package com.example.elasticsearchdemo.esdemo7;

import com.alibaba.fastjson.JSONValidator;
import com.example.elasticsearchdemo.esapi.EsBaseOperator;
import com.example.elasticsearchdemo.esapi.EsIndexOperator;
import com.example.elasticsearchdemo.esapi.EsQueryOperator;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.*;

/**
 * @program: elasticsearch-demo
 * @date: 2022/1/23
 * @author: gaorunding1
 * @description: 7.x版本操作类，采用highLevelClient
 **/
public class EsOperatorImpl implements EsIndexOperator, EsQueryOperator, EsBaseOperator<RestHighLevelClient> {

    private RestHighLevelClient client;
    private String clusterName;
    private String password;
    private String nodeIp;
    private int nodePort;

    public EsOperatorImpl(String clusterName, String password, String nodeIp, int nodePort) {
        this.clusterName = clusterName;
        this.password = password;
        this.nodeIp = nodeIp;
        this.nodePort = nodePort;
    }

    @Override
    public void initClient() {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(nodeIp, nodePort, "http"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(init()));
        client = new RestHighLevelClient(restClientBuilder);
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public RestHighLevelClient getClient() {
        return client;
    }

    @Override
    public boolean existIndex(String indexName) {
        try {
            return client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("existIndex-{}执行异常", indexName, e);
            return false;
        }
    }

    @Override
    public boolean createIndex(String indexName) {
        CreateIndexResponse createIndexResponse;
        try {
            createIndexResponse = client.indices().create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("创建索引{}异常", indexName, e);
            return false;
        }
        return createIndexResponse.isAcknowledged();
    }

    @Override
    public boolean createIndexWithSettingsAndMappings(String indexName, String settingJson, String mappingJson) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(settingJson, XContentType.JSON);
        createIndexRequest.mapping(mappingJson, XContentType.JSON);
        CreateIndexResponse createIndexResponse;
        try {
            createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("创建索引{}异常", indexName, e);
            return false;
        }
        return createIndexResponse.isAcknowledged();
    }

    @Override
    public boolean deleteIndex(String indexName) {
        AcknowledgedResponse delete;
        try {
            delete = client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("删除索引{}异常", indexName, e);
            return false;
        }
        return delete.isAcknowledged();
    }

    @Override
    public boolean createDoc(String indexName, String docJson) {
        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.source(docJson, XContentType.JSON);
        IndexResponse index;
        try {
            index = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("索引{}创建文档({})异常", indexName, docJson, e);
            return false;
        }
        return index.status() == RestStatus.CREATED;
    }

    @Override
    public boolean createDocWithId(String indexName, String docId, String docJson) {
        IndexRequest indexRequest = new IndexRequest(indexName).source(docJson, XContentType.JSON).id(docId);
        IndexResponse index;
        try {
            index = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("索引{}创建文档{}异常", indexName, docId, e);
            return false;
        }
        return index.status() == RestStatus.CREATED;
    }

    @Override
    public boolean updateDoc(String indexName, String docId, String docJson) {
        UpdateRequest updateRequest = new UpdateRequest(indexName, docId).doc(docJson, XContentType.JSON);
        UpdateResponse update;
        try {
            update = client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("索引{}更新文档{}异常", indexName, docId, e);
            return false;
        }
        return update.status() == RestStatus.OK;
    }

    @Override
    public boolean upsertDoc(String indexName, String docId, String docJson) {
        UpdateRequest updateRequest = new UpdateRequest(indexName, docId).doc(docJson, XContentType.JSON).docAsUpsert(true);
        UpdateResponse update;
        try {
            update = client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("索引{}更新文档{}异常", indexName, docId, e);
            return false;
        }
        return update.status() == RestStatus.OK || update.status() == RestStatus.CREATED;
    }

    @Override
    public boolean deleteIndexDoc(String indexName, String docId) {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, docId);
        DeleteResponse delete;
        try {
            delete = client.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("索引{}删除文档{}失败", indexName, docId);
            return false;
        }
        return delete.status() == RestStatus.OK;
    }

    @Override
    public void bulkRequest() {
        BulkRequest bulkRequest = new BulkRequest();
        //bulkRequest.add(new IndexRequest());
        if (bulkRequest.numberOfActions() <= 0) {
            return;
        }
        try {
            client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("bulk操作异常", e);
        }
    }

    @Override
    public String getDoc(String indexName, String docId) {
        GetRequest getRequest = new GetRequest(indexName, docId);
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("获取索引{}文档{}异常", indexName, docId, e);
            return null;
        }
        return getResponse.getSourceAsString();
    }

    @Override
    public List<String> mulitGet(String indexName, String... docIds) {
        if (docIds.length <= 0) {
            return new LinkedList<>();
        }
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (String docId : docIds) {
            multiGetRequest.add(indexName, docId);
        }
        MultiGetResponse mget;
        try {
            mget = client.mget(multiGetRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("mget获取索引{}文档{}异常", indexName, Arrays.toString(docIds), e);
            return null;
        }
        List<String> res = new LinkedList<>();
        MultiGetItemResponse[] multiGetItemResponses = mget.getResponses();
        for (MultiGetItemResponse multiGetItemRespons : multiGetItemResponses) {
            res.add(multiGetItemRespons.getResponse().isExists() ? multiGetItemRespons.getResponse().getSourceAsString() : null);
        }
        return res;
    }

    @Override
    public List<String> search(String indexName, String queryJson) {
        SearchRequest searchRequest = new SearchRequest(indexName);
        buildQueryJson(queryJson, searchRequest);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("执行索引{}search:{}时异常", indexName, queryJson, e);
            return null;
        }
        return getResStrings(searchResponse);
    }


    @Override
    public List<String> searchPageByFromSize(String indexName, String queryJson, int pageNum, int pageSize) {
        if (pageNum < 0) {
            pageNum = 0;
        }
        if (pageSize < 0) {
            pageSize = 10;
        }
        if (pageNum * pageSize > 10_000) {
            throw new RuntimeException("该方法只允许查询分页数在1w以内的数据");
        }
        SearchRequest searchRequest = new SearchRequest(indexName);
        buildQueryJson(queryJson, searchRequest);
        searchRequest.source().from((pageNum - 1) * pageSize).size(pageSize);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("执行索引{}search:{}时异常,from:{},size:{}", indexName, queryJson, (pageNum - 1) * pageSize, pageSize, e);
            return null;
        }
        return getResStrings(searchResponse);
    }

    @Override
    public List<String> searchPageByScroll(String indexName, String queryJson, int scrollMinute, int pageNum, int pageSize) {
        if (pageNum <= 0) {
            pageNum = 1;
        }
        if (pageSize <= 0) {
            pageSize = 10;
        }
        SearchRequest searchRequest = new SearchRequest(indexName).scroll(TimeValue.timeValueMinutes(scrollMinute));
        buildQueryJson(queryJson, searchRequest);
        searchRequest.source().size(pageSize);
        SearchResponse searchResponse;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("执行索引{}search:{}时异常,from:{},size:{}", indexName, queryJson, (pageNum - 1) * pageSize, pageSize, e);
            return null;
        }
        //如果pageNum页已经超过了数据最大量，那么直接pageNum页直接返回空列表
        if (searchResponse.getSuccessfulShards() <= 0 || searchResponse.getHits().getTotalHits().value <= (long) (pageNum - 1) * pageSize) {
            return new LinkedList<>();
        }
        //for循环查询下一页。其实吧，searchAfter和scroll都不适合深度分页。跳页查询其实都会遍历前边的数据，产品前端最好做些折衷比较适合，
        // 让用户只能一页一页翻。mysql好歹能通过b+tree主键索引进行分页优化。而es因为是通过对docId进行hash定位的，本身分页查询就没特别好的方式
        for (int i = 1; i < pageNum; i++) {
            try {
                searchResponse = client.scroll(new SearchScrollRequest(searchResponse.getScrollId()), RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("执行索引{}scroll:{}时异常,scrollId:{}", indexName, queryJson, searchResponse.getScrollId(), e);
                return null;
            }
        }
        //最后可以clear掉当前的scroll，减少es服务端内存占用。或者当快照到期自然clear也成
        //client.prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();
        return getResStrings(searchResponse);
    }

    @Override
    public List<String> searchPageBySearchAfter(String indexName, String queryJson, int pageNum, int pageSize) {
        if (pageNum <= 0) {
            pageNum = 1;
        }
        if (pageSize <= 0) {
            pageSize = 10;
        }
        SearchRequest searchRequest = new SearchRequest(indexName);
        buildQueryJson(queryJson, searchRequest);
        searchRequest.source().sort("_index", SortOrder.ASC).sort("_id", SortOrder.ASC).size(pageSize);
        SearchResponse searchResponse;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("执行索引{}searchAfter:{}时异常", indexName, queryJson, e);
            return null;
        }
        //如果pageNum页已经超过了数据最大量，那么直接pageNum页直接返回空列表
        if (searchResponse.getSuccessfulShards() <= 0 || searchResponse.getHits().getTotalHits().value <= (long) (pageNum - 1) * pageSize) {
            return new LinkedList<>();
        }
        try {
            //for循环查询下一页。其实吧，searchAfter和scroll都不适合深度分页。跳页查询其实都会遍历前边的数据，产品前端最好做些折衷比较适合，
            // 让用户只能一页一页翻。mysql好歹能通过b+tree主键索引进行分页优化。而es因为是通过对docId进行hash定位的，本身分页查询就没特别好的方式
            for (int i = 1; i < pageNum; i++) {
                SearchHit[] hits = searchResponse.getHits().getHits();
                Object[] sortValues = hits[hits.length - 1].getSortValues();
                searchRequest.source().searchAfter(sortValues);
                searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            }
        } catch (IOException e) {
            log.error("执行索引{}searchAfter:{}时异常", indexName, queryJson, e);
            return null;
        }
        return getResStrings(searchResponse);
    }

    @Override
    public List<String> searchWithSort(String indexName, String queryJson, String... sortFields) {
        SearchRequest searchRequest = new SearchRequest(indexName);
        buildQueryJson(queryJson, searchRequest);
        for (String sortField : sortFields) {
            searchRequest.source().sort(sortField, SortOrder.ASC);
        }
        SearchResponse searchResponse;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("执行索引{}search:{}时异常,sort:{}", indexName, queryJson, Arrays.toString(sortFields), e);
            return null;
        }
        return getResStrings(searchResponse);
    }

    @Override
    public Double searchWithMetricAggregation(String indexName, String sumField) {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .aggregation(AggregationBuilders.sum("sum").field(sumField));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("search索引{}时异常", indexName, e);
            return null;
        }
        Sum sum = searchResponse.getAggregations().get("sum");
        return sum.getValue();
    }

    @Override
    public Map<String, Long> searchWithBulkAggregation(String indexName, String groupField) {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .aggregation(AggregationBuilders.terms("groupby").field(groupField));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("search索引{}时异常", indexName, e);
            return null;
        }
        Terms terms = searchResponse.getAggregations().get("groupby");
        Map<String, Long> resMap = new HashMap<>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            resMap.put(bucket.getKeyAsString(), bucket.getDocCount());
        }
        return resMap;
    }

    @Override
    public Map<String, Double> searchWithPipelineAggregation(String indexName, String groupField, String sumField) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .aggregation(AggregationBuilders.terms("groupby").field(groupField)
                        .subAggregation(AggregationBuilders.sum("sum").field(sumField)));
        SearchRequest searchRequest = new SearchRequest(indexName).source(searchSourceBuilder);
        SearchResponse searchResponse;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("search索引{}时异常", indexName, e);
            return null;
        }
        Terms terms = searchResponse.getAggregations().get("groupby");
        Map<String, Double> resMap = new HashMap<>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Sum sum = bucket.getAggregations().get("sum");
            resMap.put(bucket.getKeyAsString(), sum.getValue());
        }
        return resMap;
    }

    @Override
    public List<String> multiSearchWithIndexNames(String... indexNames) {
        SearchResponse search;
        try {
            search = client.search(new SearchRequest(indexNames), RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("multiSearch异常，indexNames为:{}", Arrays.toString(indexNames), e);
            return null;
        }
        return getResStrings(search);
    }

    @Override
    public List<List<String>> multiSearchWithQueryJsons(String indexName, String... queryJson) {
        if (queryJson.length<=0){
            return new LinkedList<>();
        }
        MultiSearchRequest multiSearchRequest=new MultiSearchRequest();
        for (String singleQueryJson : queryJson) {
            if (StringUtils.isNotEmpty(singleQueryJson)&& !JSONValidator.from(singleQueryJson).validate()){
                throw new RuntimeException("传入的queryJson不能转换为json串，请检查");
            }
            SearchRequest searchRequest=new SearchRequest(indexName);
            buildQueryJson(singleQueryJson,searchRequest);
            searchRequest.source().size(100);
            multiSearchRequest.add(searchRequest);
        }
        if (multiSearchRequest.requests().size()<=0){
            return new LinkedList<>();
        }
        MultiSearchResponse.Item[] responses = new MultiSearchResponse.Item[0];
        try {
            responses = client.msearch(multiSearchRequest, RequestOptions.DEFAULT).getResponses();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        List<List<String>> res=new ArrayList<>();
        for (MultiSearchResponse.Item respons : responses) {
            if(respons.isFailure()){
                res.add(null);
                continue;
            }
            List<String> resStrings = getResStrings(respons.getResponse());
            res.add(resStrings);
        }
        return res;
    }

    @Override
    public void asyncSearch(String indexName) {
        client.searchAsync(new SearchRequest(), RequestOptions.DEFAULT, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                List<String> resStrings = getResStrings(searchResponse);
                log.info("输出结果为:{}", resStrings);
            }

            @Override
            public void onFailure(Exception e) {
                log.error("异步请求出错", e);
            }
        });
    }

    private CredentialsProvider init() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(clusterName, password));
        return credentialsProvider;
    }

    /**
     * 解析searchResponse获取返回结果
     *
     * @param searchResponse
     * @return
     */
    private List<String> getResStrings(SearchResponse searchResponse) {
        if (searchResponse == null || searchResponse.getSuccessfulShards() <= 0 || searchResponse.getHits().getTotalHits().value <= 0) {
            return new LinkedList<>();
        }
        List<String> res = new LinkedList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            res.add(hit.getSourceAsString());
        }
        return res;
    }


    /**
     * 构建queryBuilder
     *
     * @param queryJson
     * @param searchRequest
     */
    private void buildQueryJson(String queryJson, SearchRequest searchRequest) {
        //一般用queryBuilder自己构建就成-------
        //QueryBuilder queryBuilder=new TermQueryBuilder("field","value");
        //searchRequestBuilder.setQuery(queryBuilder);
        //wrapperQuery只会包含查询json中query下的部分，像别的aggs、sort这些key下面的string是不会解析的-----
        if (StringUtils.isNotEmpty(queryJson)) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.wrapperQuery(queryJson));
            searchRequest.source(searchSourceBuilder);
        }
    }
}
