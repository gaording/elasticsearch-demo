package com.example.elasticsearchdemo.esdemo7;

import com.alibaba.fastjson.JSONObject;
import com.example.elasticsearchdemo.esapi.entity.TestPojo;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @program: elasticsearch-demo
 * @date: 2022/1/20
 * @author: gaorunding1
 * @description:
 **/
public class EsOperatorImplTest {

    public static final Logger log = LoggerFactory.getLogger(EsOperatorImplTest.class);


    //自己对应的集群name
    private static final String clusterName="";
    //自己对应的password
    public static final String password="";
    //自己对应的nodeIp
    public static final String nodeId="";
    //自己对应的port
    public static final int port=20100;

    /**
     * es操作类
     */
    private final EsOperatorImpl esOperator = new EsOperatorImpl(clusterName, password, nodeId, port);
    /**
     * es索引
     */
    private static final String INDEX_NAME = "grd-test-index";
    public static final String DEFAULT_SETTINGs = "{\"number_of_shards\":\"3\",\"number_of_replicas\":\"1\"}";
    /**
     * 比6.3少了个_all配置项
     */
    public static final String DEFAULT_MAPPINGS = "{\"dynamic\":true,\"_source\":{\"enabled\":true},\"properties\":{\"user\":{\"type\":\"keyword\"},\"age\":{\"type\":\"integer\"},\"content\":{\"type\":\"text\",\"analyzer\":\"ik_smart\"},\"date\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.s||yyyy-MM-dd||epoch_millis\"}}}";

    @BeforeEach
    public void beforeAll() throws Exception {
        esOperator.initClient();
    }

    @AfterEach
    public void afterAll() throws Exception {
        esOperator.close();
    }

    @Nested
    class TestCreateIndex {

        @BeforeEach
        public void before() {
            boolean exist = esOperator.existIndex(INDEX_NAME);
            if (exist) {
                log.info("索引{}已存在，先进行删除", INDEX_NAME);
                boolean b = esOperator.deleteIndex(INDEX_NAME);
                log.info("索引{}删除结果为:{}", INDEX_NAME, b);
            }
        }


        @Test
        public void testCreateIndex() {
            Assertions.assertTrue(esOperator.createIndexWithSettingsAndMappings(INDEX_NAME, DEFAULT_SETTINGs, DEFAULT_MAPPINGS), "索引创建失败");
        }

        @Test
        public void testCreateIndexWithSettingsAndMappings() {
            esOperator.createIndexWithSettingsAndMappings(INDEX_NAME, DEFAULT_SETTINGs, DEFAULT_MAPPINGS);
            Assertions.assertTrue(esOperator.existIndex(INDEX_NAME));
        }

    }

    @Nested
    class TestDeleteIndex {

        @BeforeEach
        public void before() {
            boolean exist = esOperator.existIndex(INDEX_NAME);
            if (!exist) {
                log.info("索引{}不存在，先进行创建", INDEX_NAME);
                boolean b = esOperator.createIndexWithSettingsAndMappings(INDEX_NAME, DEFAULT_SETTINGs, DEFAULT_MAPPINGS);
                log.info("索引{}创建结果为:{}", INDEX_NAME, b);
            }
        }


        @Test
        public void testDeleteIndex() {
            esOperator.deleteIndex(INDEX_NAME);
            Assertions.assertFalse(esOperator.existIndex(INDEX_NAME));
        }

    }

    @Nested
    class TestDoc {
        TestPojo testPojo = new TestPojo();

        @BeforeEach
        public void before() {
            if (!esOperator.existIndex(INDEX_NAME)) {
                esOperator.createIndexWithSettingsAndMappings(INDEX_NAME, DEFAULT_SETTINGs, DEFAULT_MAPPINGS);
            }
            if (StringUtils.isNotEmpty(esOperator.getDoc(INDEX_NAME,"1"))){
                esOperator.deleteIndexDoc(INDEX_NAME,"1");
            }
            testPojo.setUser("grd");
            testPojo.setAge(18);
            testPojo.setContent("daydayup");
            testPojo.setDate(LocalDateTime.now(ZoneOffset.systemDefault()));
        }

        @Test
        public void testCreateDoc() {
            Assertions.assertTrue(esOperator.createDoc(INDEX_NAME, JSONObject.toJSONString(testPojo)), "doc创建失败");
        }

        @Test
        public void testCreateDocWithId() {
            Assertions.assertTrue(esOperator.createDocWithId(INDEX_NAME, "1", JSONObject.toJSONString(testPojo)), "doc创建失败");
        }
    }

    @Nested
    class TestUpdate {
        TestPojo testPojo = new TestPojo();

        @BeforeEach
        public void before() {
            if (!esOperator.existIndex(INDEX_NAME)) {
                esOperator.createIndexWithSettingsAndMappings(INDEX_NAME, DEFAULT_SETTINGs, DEFAULT_MAPPINGS);
            }
            if (StringUtils.isBlank(esOperator.getDoc(INDEX_NAME, "1"))) {
                //先新增
                testPojo.setUser("grd");
                testPojo.setAge(20);
                esOperator.createDocWithId(INDEX_NAME, "1", JSONObject.toJSONString(testPojo));
            }
            if (StringUtils.isNotBlank(esOperator.getDoc(INDEX_NAME, "upsert-1"))) {
                testPojo.setUser("grd'girlfriend");
                testPojo.setAge(20);
                esOperator.deleteIndexDoc(INDEX_NAME, "upsert-1");

            }
        }

        @Test
        public void testUpdate() {
            Assertions.assertTrue(esOperator.updateDoc(INDEX_NAME, "1", JSONObject.toJSONString(testPojo)), "更新doc失败");
        }

        @Test
        public void testUpsert() {
            Assertions.assertTrue(esOperator.upsertDoc(INDEX_NAME, "upsert-1", JSONObject.toJSONString(testPojo)), "更新doc失败");
        }
    }

    @Nested
    class TestDeleteDoc {
        @BeforeEach
        public void before() {
            if (!esOperator.existIndex(INDEX_NAME)) {
                esOperator.createIndexWithSettingsAndMappings(INDEX_NAME, DEFAULT_SETTINGs, DEFAULT_MAPPINGS);
            }
            if (StringUtils.isBlank(esOperator.getDoc(INDEX_NAME, "1"))) {
                TestPojo testPojo = new TestPojo();
                //先新增
                testPojo.setUser("grd");
                testPojo.setAge(20);
                esOperator.createDocWithId(INDEX_NAME, "1", JSONObject.toJSONString(testPojo));
            }
        }

        @Test
        public void testDeleteIndexDoc() {
            Assertions.assertTrue(esOperator.deleteIndexDoc(INDEX_NAME, "1"));
        }
    }


    @Nested
    class TestGetOrMultiGet {
        @BeforeEach
        public void before() {
            if (!esOperator.existIndex(INDEX_NAME)) {
                esOperator.createIndexWithSettingsAndMappings(INDEX_NAME, DEFAULT_SETTINGs, DEFAULT_MAPPINGS);
            }
            BulkRequest bulkRequest=new BulkRequest();
            for (int i = 0; i < 2; i++) {
                bulkRequest.add(new DeleteRequest(INDEX_NAME,String.format("grd-%d", i)));
            }
            try {
                esOperator.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            bulkRequest=new BulkRequest();
            for (int i = 0; i < 2; i++) {
                TestPojo testPojo = new TestPojo();
                testPojo.setUser(String.format("grd-%d", i));
                bulkRequest.add(new IndexRequest(INDEX_NAME).id(testPojo.getUser()).source(JSONObject.toJSONString(testPojo), XContentType.JSON));
            }
            try {
                esOperator.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Test
        public void testGetDoc() {
            Assertions.assertNotNull(esOperator.getDoc(INDEX_NAME, "grd-1"));
        }

        @Test
        public void testMulitGet() {
            Assertions.assertEquals(2, esOperator.mulitGet(INDEX_NAME, "grd-1", "grd-2").size());
        }
    }


    @Nested
    class TestSearch {

        @BeforeEach
        public void before() {
            if (!esOperator.existIndex(INDEX_NAME)) {
                esOperator.createIndexWithSettingsAndMappings(INDEX_NAME, DEFAULT_SETTINGs, DEFAULT_MAPPINGS);
            }
            BulkRequest bulkRequest=new BulkRequest();
            for (int i = 0; i < 2; i++) {
                bulkRequest.add(new DeleteRequest(INDEX_NAME,String.format("grd-%d", i)));
            }
            try {
                esOperator.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            bulkRequest=new BulkRequest();
            for (int i = 0; i < 2; i++) {
                TestPojo testPojo = new TestPojo();
                testPojo.setUser(String.format("grd-%d", i));
                bulkRequest.add(new IndexRequest(INDEX_NAME).id(testPojo.getUser()).source(JSONObject.toJSONString(testPojo), XContentType.JSON));
            }
            try {
                esOperator.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Test
        public void testSearch() {
            XContentBuilder queryXcontent = null;
            try {
                queryXcontent = jsonBuilder().startObject()
                        .field("match_all")
                        .startObject().endObject()
                        .endObject();
            } catch (IOException e) {
                e.printStackTrace();
            }
            String queryJson = queryXcontent == null ? null : Strings.toString(queryXcontent);
            Assertions.assertTrue(esOperator.search(INDEX_NAME, queryJson).size() > 0);
        }

        @Test
        public void testSearchPageByFromSize() {
            Assertions.assertEquals(1, esOperator.searchPageByFromSize(INDEX_NAME, null, 2, 1).size());
        }

        @Test
        public void testSearchPageByScroll() {
            Assertions.assertEquals(1, esOperator.searchPageByScroll(INDEX_NAME, null, 1, 2, 1).size());
        }

        @Test
        public void testSearchPageBySearchAfter() {
            Assertions.assertEquals(1, esOperator.searchPageBySearchAfter(INDEX_NAME, null, 2, 1).size());
        }

        @Test
        public void testSearchWithSort() {
            List<String> resJson = esOperator.searchWithSort(INDEX_NAME, null, "user");
            List<String> user=resJson.stream().map(itemJson->JSONObject.parseObject(itemJson,TestPojo.class)).map(TestPojo::getUser).collect(Collectors.toList());
            List<String> sortUser = new ArrayList<>(user);
            Collections.sort(sortUser);
            for (int i = 0; i < user.size(); i++) {
                Assertions.assertEquals(user.get(i), sortUser.get(i));
            }
        }

    }

    @Nested
    class TestAggregation {
        @BeforeEach
        public void before() {
            if (!esOperator.existIndex(INDEX_NAME)) {
                esOperator.createIndexWithSettingsAndMappings(INDEX_NAME, DEFAULT_SETTINGs, DEFAULT_MAPPINGS);
            }
            if (StringUtils.isEmpty(esOperator.getDoc(INDEX_NAME, "1"))) {
                TestPojo testPojo = new TestPojo();
                //先新增
                testPojo.setUser("grd");
                testPojo.setAge(20);
                esOperator.createDocWithId(INDEX_NAME, "1", JSONObject.toJSONString(testPojo));
            }
        }

        @Test
        public void testSearchWithMetricAggregation() {
            Double age = esOperator.searchWithMetricAggregation(INDEX_NAME, "age");
            Assertions.assertNotNull(age);
        }

        @Test
        public void testSearchWithBulkAggregation() {
            Map<String, Long> user = esOperator.searchWithBulkAggregation(INDEX_NAME, "user");
            Assertions.assertTrue(user.size() > 0);
        }

        @Test
        public void testSearchWithPipelineAggregation() {
            Map<String, Double> stringDoubleMap = esOperator.searchWithPipelineAggregation(INDEX_NAME, "user", "age");
            Assertions.assertTrue(stringDoubleMap.size() > 0);
        }
    }

    @Nested
    class TestMultiSearch {
        @BeforeEach
        public void before() {
            if (!esOperator.existIndex(INDEX_NAME)) {
                esOperator.createIndexWithSettingsAndMappings(INDEX_NAME, DEFAULT_SETTINGs, DEFAULT_MAPPINGS);
            }
            if (StringUtils.isBlank(esOperator.getDoc(INDEX_NAME, "1"))) {
                TestPojo testPojo = new TestPojo();
                //先新增
                testPojo.setUser("grd");
                testPojo.setAge(20);
                esOperator.createDocWithId(INDEX_NAME, "1", JSONObject.toJSONString(testPojo));
            }
            String indexName2 = INDEX_NAME + "1";
            if (!esOperator.existIndex(indexName2)) {
                esOperator.createIndex(indexName2);
            }
            if (StringUtils.isBlank(esOperator.getDoc(indexName2, "1"))) {
                TestPojo testPojo = new TestPojo();
                //先新增
                testPojo.setUser("grd");
                testPojo.setAge(20);
                esOperator.createDocWithId(indexName2, "1", JSONObject.toJSONString(testPojo));
            }
        }


        @Test
        public void testMultiSearchWithIndexNames() {
            List<String> strings = esOperator.multiSearchWithIndexNames(INDEX_NAME, INDEX_NAME + "1");
            Assertions.assertTrue(strings.size() > 0);
        }

        @Test
        public void testMultiSearchWithQueryJsons() {
            List<List<String>> strings = esOperator.multiSearchWithQueryJsons(INDEX_NAME, null,"{\"term\":{\"user\":\"grd\"}}","{\"term\":{\"age\":\"20\"}}");
            Assertions.assertEquals(3, strings.size());
        }
    }


    @Test
    public void testAsyncSearch() {
    }


}