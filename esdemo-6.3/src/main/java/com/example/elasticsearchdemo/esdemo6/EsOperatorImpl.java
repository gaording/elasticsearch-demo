package com.example.elasticsearchdemo.esdemo6;

import com.alibaba.fastjson.JSONValidator;
import com.example.elasticsearchdemo.esapi.EsBaseOperator;
import com.example.elasticsearchdemo.esapi.EsIndexOperator;
import com.example.elasticsearchdemo.esapi.EsQueryOperator;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @program: elasticsearch-demo
 * @date: 2022/1/19
 * @author: gaorunding1
 * @description: es?????????6.3????????????
 * ?????????????????????https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.8/index.html
 **/
public class EsOperatorImpl implements EsIndexOperator, EsQueryOperator, EsBaseOperator<Client> {

    /**
     * 6.3??????transport?????????
     */
    private Client client;
    /**
     * ?????????
     */
    private String clusterName;
    /**
     * ??????
     */
    private String password;
    /**
     * ??????
     */
    private String nodeIp;

    /**
     * ??????
     */
    private int nodePort;

    public EsOperatorImpl(String clusterName, String password, String nodeIp, int nodePort) {
        this.clusterName = clusterName;
        this.password = password;
        this.nodeIp = nodeIp;
        this.nodePort = nodePort;
    }

    @Override
    public void initClient() {
        client = getClient(clusterName, password, nodeIp, nodePort);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public boolean existIndex(String indexName) {
        IndicesExistsResponse indicesExistsResponse = client.admin().indices().prepareExists(indexName).get();
        return indicesExistsResponse.isExists();
    }

    @Override
    public boolean createIndex(String indexName) {
        CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(indexName).get();
        return createIndexResponse.isAcknowledged();
    }

    @Override
    public boolean createIndexWithSettingsAndMappings(String indexName, String settingJson, String mappingJson) {
        CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(indexName)
                .setSettings(settingJson, XContentType.JSON)
                .addMapping(DEFAULT_TYPE, mappingJson, XContentType.JSON)
                .get();
        return createIndexResponse.isAcknowledged();
    }

    @Override
    public boolean deleteIndex(String indexName) {
        DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(indexName).get();
        return deleteIndexResponse.isAcknowledged();
    }

    @Override
    public boolean createDoc(String indexName, String docJson) {
        IndexResponse indexResponse = client.prepareIndex(indexName, DEFAULT_TYPE).setSource(docJson, XContentType.JSON).get();
        log.info("??????doc??????,???????????????docId???{}", indexResponse.getId());
        return indexResponse.status() == RestStatus.CREATED;
    }

    @Override
    public boolean createDocWithId(String indexName, String docId, String docJson) {
        IndexResponse indexResponse = client.prepareIndex(indexName, DEFAULT_TYPE, docId).setSource(docJson, XContentType.JSON).get();
        return indexResponse.status() == RestStatus.CREATED;
    }

    @Override
    public boolean updateDoc(String indexName, String docId, String docJson) {
        UpdateResponse response = client.prepareUpdate(indexName, DEFAULT_TYPE, docId)
                .setDoc(docJson, XContentType.JSON)
                .get();
        return response.status() == RestStatus.OK;
    }

    @Override
    public boolean upsertDoc(String indexName, String docId, String docJson) {
        UpdateResponse response = client.prepareUpdate(indexName, DEFAULT_TYPE, docId)
                .setDoc(docJson, XContentType.JSON)
                .setDocAsUpsert(true)
                .get();
        return response.status() == RestStatus.CREATED || response.status() == RestStatus.OK;
    }

    @Override
    public boolean deleteIndexDoc(String indexName, String docId) {
        DeleteResponse deleteResponse = client.prepareDelete(indexName, DEFAULT_TYPE, docId).get();
        return deleteResponse.status() == RestStatus.OK;
    }

    @Override
    public void bulkRequest() {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        //add WriteRequestBuilder
        //bulkRequestBuilder.add()
        if (bulkRequestBuilder.numberOfActions() <= 0) {
            log.info("bulk??????????????????????????????");
        }
        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        for (int i = 0; i < bulkItemResponses.getItems().length; i++) {
            BulkItemResponse item = bulkItemResponses.getItems()[i];
            log.info("???{}?????????fail?{}", i, item.isFailed());
        }
    }


    @Override
    public String getDoc(String indexName, String docId) {
        GetResponse getResponse = client.prepareGet(indexName, DEFAULT_TYPE, docId).get();
        //???????????????sourceAsString?????????????????????jsonObject??????????????????????????????
        // ??????es????????????????????????store?????????????????????????????????store??????
        return getResponse.isExists() ? getResponse.getSourceAsString() : null;
    }

    @Override
    public List<String> mulitGet(String indexName, String... docIds) {
        if (docIds.length <= 0) {
            return new LinkedList<>();
        }
        MultiGetRequestBuilder multiGetRequestBuilder = client.prepareMultiGet();
        for (String docId : docIds) {
            multiGetRequestBuilder.add(indexName, DEFAULT_TYPE, docId);
        }
        List<String> res = new LinkedList<>();
        MultiGetResponse multiGetItemResponses = multiGetRequestBuilder.get();
        for (MultiGetItemResponse multiGetItemRespons : multiGetItemResponses) {
            res.add(multiGetItemRespons.getResponse().isExists() ? multiGetItemRespons.getResponse().getSourceAsString() : null);
        }
        return res;
    }

    @Override
    public List<String> search(String indexName, String queryJson) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                //size???????????????????????????10?????????????????????
                .setSize(10_000);
        //??????QueryBuilder
        buildQueryBuilder(queryJson, searchRequestBuilder);
        SearchResponse searchResponse = searchRequestBuilder.get();
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
            throw new RuntimeException("????????????????????????????????????1w???????????????");
        }
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setFrom((pageNum - 1) * pageSize)
                .setSize(pageSize);
        //?????????queryBuilder??????????????????-------
        //QueryBuilder queryBuilder=new TermQueryBuilder("field","value");
        //searchRequestBuilder.setQuery(queryBuilder);
        //wrapperQuery??????????????????json???query????????????????????????aggs???sort??????key?????????string??????????????????-----
        buildQueryBuilder(queryJson, searchRequestBuilder);
        SearchResponse searchResponse = searchRequestBuilder.get();
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
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setScroll(TimeValue.timeValueMinutes(scrollMinute))
                .setSize(pageSize);
        //??????queryBuilder
        buildQueryBuilder(queryJson, searchRequestBuilder);
        SearchResponse searchResponse = searchRequestBuilder.get();
        //??????pageNum????????????????????????????????????????????????pageNum????????????????????????
        if (searchResponse.getSuccessfulShards() <= 0 || searchResponse.getHits().totalHits <= (long) (pageNum - 1) * pageSize) {
            return new LinkedList<>();
        }
        //for????????????????????????????????????searchAfter???scroll????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
        // ?????????????????????????????????mysql???????????????b+tree????????????????????????????????????es??????????????????docId??????hash??????????????????????????????????????????????????????
        for (int i = 1; i < pageNum; i++) {
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).get();
        }
        //????????????clear????????????scroll?????????es???????????????????????????????????????????????????clear??????
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
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setSize(pageSize)
                .addSort("_index", SortOrder.ASC)
                .addSort("_id", SortOrder.ASC);
        buildQueryBuilder(queryJson, searchRequestBuilder);
        SearchResponse searchResponse = searchRequestBuilder.get();
        //??????pageNum????????????????????????????????????????????????pageNum????????????????????????
        if (searchResponse.getSuccessfulShards() <= 0 || searchResponse.getHits().totalHits <= (long) (pageNum - 1) * pageSize) {
            return new LinkedList<>();
        }
        //for????????????????????????????????????searchAfter???scroll????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
        // ?????????????????????????????????mysql???????????????b+tree????????????????????????????????????es??????????????????docId??????hash??????????????????????????????????????????????????????
        for (int i = 1; i < pageNum; i++) {
            SearchHit[] hits = searchResponse.getHits().getHits();
            Object[] sortValues = hits[hits.length - 1].getSortValues();
            searchResponse = searchRequestBuilder.searchAfter(sortValues).get();
        }
        return getResStrings(searchResponse);
    }


    @Override
    public List<String> searchWithSort(String indexName, String queryJson, String... sortFields) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setSize(10_000);
        buildQueryBuilder(queryJson, searchRequestBuilder);
        for (String sortField : sortFields) {
            searchRequestBuilder.addSort(sortField, SortOrder.ASC);
        }
        SearchResponse searchResponse = searchRequestBuilder.get();
        return getResStrings(searchResponse);
    }

    @Override
    public Double searchWithMetricAggregation(String indexName, String sumField) {
        //?????????????????????????????????aggs?????????????????????
        AggregationBuilder aggregationBuilder = AggregationBuilders.sum("sum").field(sumField);
        SearchResponse searchResponse = client.prepareSearch(indexName)
                //??????size???0???????????????hits???????????????aggregation?????????
                .setSize(0)
                .addAggregation(aggregationBuilder)
                .get();
        if (searchResponse.getSuccessfulShards() <= 0) {
            return null;
        }
        Aggregations aggregations = searchResponse.getAggregations();
        Sum sum = aggregations.get("sum");
        return sum.getValue();
    }

    @Override
    public Map<String, Long> searchWithBulkAggregation(String indexName, String groupField) {
        //?????????????????????????????????aggs?????????????????????
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("groupBy").field(groupField);
        SearchResponse searchResponse = client.prepareSearch(indexName)
                //??????size???0???????????????hits???????????????aggregation?????????
                .setSize(0)
                .addAggregation(aggregationBuilder)
                .get();
        Map<String, Long> groupFieldAndCountMap = new HashMap<>();
        if (searchResponse.getSuccessfulShards() <= 0) {
            return groupFieldAndCountMap;
        }
        //groupby???????????????
        Aggregations aggregations = searchResponse.getAggregations();
        Terms terms = aggregations.get("groupBy");
        for (Terms.Bucket bucket : terms.getBuckets()) {
            groupFieldAndCountMap.put(bucket.getKeyAsString(), bucket.getDocCount());
        }
        return groupFieldAndCountMap;
    }


    @Override
    public Map<String, Double> searchWithPipelineAggregation(String indexName, String groupField, String sumField) {
        //?????????????????????????????????aggs?????????????????????
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("groupBy").field(groupField);
        aggregationBuilder.subAggregation(AggregationBuilders.sum("sum").field(sumField));
        SearchResponse searchResponse = client.prepareSearch(indexName)
                //??????size???0???????????????hits???????????????aggregation?????????
                .setSize(0)
                .addAggregation(aggregationBuilder)
                .get();
        Map<String, Double> groupFieldAndSumMap = new HashMap<>();
        if (searchResponse.getSuccessfulShards() <= 0) {
            return groupFieldAndSumMap;
        }
        Terms terms = searchResponse.getAggregations().get("groupBy");
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Sum sum = bucket.getAggregations().get("sum");
            groupFieldAndSumMap.put(bucket.getKeyAsString(), sum.getValue());
        }
        return groupFieldAndSumMap;
    }


    @Override
    public List<String> multiSearchWithIndexNames(String... indexNames) {
        SearchResponse searchResponse = client.prepareSearch(indexNames)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(100)
                .get();
        return getResStrings(searchResponse);
    }

    @Override
    public List<List<String>> multiSearchWithQueryJsons(String indexName, String... queryJson) {
        if (queryJson.length <= 0) {
            return new LinkedList<>();
        }
        MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();
        for (String singleQueryJson : queryJson) {
            if (StringUtils.isNotEmpty(singleQueryJson) && !JSONValidator.from(singleQueryJson).validate()) {
                throw new RuntimeException("?????????queryJson???????????????json???????????????");
            }
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName).setSize(100);
            buildQueryBuilder(singleQueryJson, searchRequestBuilder);
            multiSearchRequestBuilder.add(searchRequestBuilder);
        }
        List<List<String>> res = new LinkedList<>();
        MultiSearchResponse multiSearchResponse = multiSearchRequestBuilder.get();
        for (MultiSearchResponse.Item respons : multiSearchResponse.getResponses()) {
            if (respons.isFailure()) {
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
        client.prepareSearch(indexName)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        List<String> resStrings = getResStrings(searchResponse);
                        log.info("???????????????:{}", resStrings);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("??????????????????", e);
                    }
                });
    }


    @Override
    public Client getClient() {
        return client;
    }

    /**
     * ??????es client ?????????????????????????????????????????????????????????????????????????????????
     * clusterName:????????????
     * nodeIp:??????????????????ip??????
     * nodePort:???????????????
     *
     * @return
     */
    private Client getClient(String clusterName, String password, String nodeIp, int nodePort) {
        //?????????????????????
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("request.headers.Authorization", basicAuthHeaderValue(clusterName, password))
                .put("client.transport.sniff", false)
                .build();

        TransportAddress transportAddress = null;
        try {
            transportAddress = new TransportAddress(InetAddress.getByName(nodeIp), nodePort);
        } catch (UnknownHostException e) {
            log.error("????????????host??????", e);
            e.printStackTrace();
        }
        Client client = new PreBuiltTransportClient(settings).addTransportAddress(transportAddress);
        return client;
    }


    private static String basicAuthHeaderValue(String username, String passwd) {
        CharBuffer chars = CharBuffer.allocate(username.length() + passwd.length() + 1);
        byte[] charBytes = null;
        try {
            chars.put(username).put(':').put(passwd.toCharArray());
            charBytes = toUtf8Bytes(chars.array());

            String basicToken = Base64.getEncoder().encodeToString(charBytes);
            return "Basic " + basicToken;
        } finally {
            Arrays.fill(chars.array(), (char) 0);
            if (charBytes != null) {
                Arrays.fill(charBytes, (byte) 0);
            }
        }
    }

    public static byte[] toUtf8Bytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        byte[] bytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        Arrays.fill(byteBuffer.array(), (byte) 0);
        return bytes;
    }

    /**
     * ??????searchResponse??????????????????
     *
     * @param searchResponse
     * @return
     */
    private List<String> getResStrings(SearchResponse searchResponse) {
        if (searchResponse.getSuccessfulShards() <= 0 || searchResponse.getHits().totalHits <= 0) {
            return new LinkedList<>();
        }
        List<String> res = new LinkedList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            res.add(hit.getSourceAsString());
        }
        return res;
    }

    /**
     * ??????QueryBuilder
     *
     * @param queryJson
     * @param searchRequestBuilder
     */
    private void buildQueryBuilder(String queryJson, SearchRequestBuilder searchRequestBuilder) {
        //?????????queryBuilder??????????????????-------
        //QueryBuilder queryBuilder=new TermQueryBuilder("field","value");
        //searchRequestBuilder.setQuery(queryBuilder);
        //wrapperQuery??????????????????json???query????????????????????????aggs???sort??????key?????????string??????????????????-----
        if (StringUtils.isNotEmpty(queryJson)) {
            if (!JSONValidator.from(queryJson).validate()) {
                throw new RuntimeException("queryJson???????????????json???????????????");
            }
            searchRequestBuilder.setQuery(QueryBuilders.wrapperQuery(queryJson));
        }
    }
}
