package com.example.elasticsearchdemo.esapi;

/**
 * @program: elasticsearch-demo
 * @date: 2022/1/19
 * @author: gaorunding1
 * @description: es的index和doc增删操作
 **/
public interface EsIndexOperator {

    /**
     * 判断索引是否存在
     *
     * @param indexName
     */
    boolean existIndex(String indexName);

    /**
     * 创建索引(type默认为_doc)
     *
     * @param indexName
     * @return
     */
    boolean createIndex(String indexName);


    /**
     * 创建带有setting和mappings的索引
     *
     * @param indexName
     * @param settingJson
     * @param mappingJson
     * @return
     */
    boolean createIndexWithSettingsAndMappings(String indexName, String settingJson, String mappingJson);

    /**
     * 删除索引
     *
     * @param indexName
     * @return
     */
    boolean deleteIndex(String indexName);

    /**
     * 在index下创建doc
     *
     * @param indexName
     * @param docJson
     * @return
     */
    boolean createDoc(String indexName, String docJson);

    /**
     * 在index下创建doc
     *
     * @param indexName
     * @param docId
     * @param docJson
     * @return
     */
    boolean createDocWithId(String indexName, String docId, String docJson);

    /**
     * 更新指定doc
     *
     * @param indexName
     * @param docJson
     * @return
     */
    boolean updateDoc(String indexName, String docId, String docJson);


    /**
     * 更新或新增
     * @param indexName
     * @param docId
     * @param docJson
     * @return
     */
    boolean upsertDoc(String indexName, String docId, String docJson);

    /**
     * 删除doc
     *
     * @param indexName
     * @param docId
     * @return
     */
    boolean deleteIndexDoc(String indexName, String docId);


    /**
     * bulk批量请求
     */
    void bulkRequest();

}
