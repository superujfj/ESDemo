package com.supershan.es;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@DisplayName("My first test case")
class ElasticSearchUtilTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchUtilTest.class);
    ElasticSearchUtil esUtil = null;


    @BeforeEach
    void setUp() {
        esUtil = new ElasticSearchUtil(new String[]{"127.0.0.1:9200"});
    }

    @AfterEach
    void tearDown() throws IOException {
        esUtil.close();
    }

    /**
     * 创建索引
     */

    @Test
    void createIndex() throws IOException {
        String index = "users";
        System.out.println(esUtil.createIndex(index) ? "创建索引成功" : "创建索引失败");
    }

    /**
     * 创建索引
     */
    @Test
    void createDoc() throws IOException {
        String index = "users";
        String docId = "1";
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("id", "1");
        jsonMap.put("name", "A");
        jsonMap.put("subject", "Chinese");
        jsonMap.put("score", 90);
        jsonMap.put("password", "A2020");

        try {
            IndexResponse response = esUtil.execIndex(index, docId, jsonMap);
            System.out.println("index: " + response.getIndex());
            System.out.println("id: " + response.getId());
            System.out.println("version: " + response.getVersion());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void getDoc() throws IOException {
        String index = "users";
        String docId = "1";

        GetResponse response = esUtil.getResponse(index, docId);

        Map<String, Object> source = response.getSource();
        Set<String> strings = source.keySet();
        Iterator<String> iterator = strings.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            System.out.println(key + ":" + source.get(key));
        }
    }

    @Test
    void updateDoc() throws IOException {
        String index = "users";
        String docId = "1";
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("id", "2");
        jsonMap.put("name", "B");
        jsonMap.put("subject", "English");
        jsonMap.put("score", 100);
        jsonMap.put("password", "B2020");

        UpdateResponse response = esUtil.updateDoc(index, docId, jsonMap);

        System.out.println("index: " + response.getIndex());
        System.out.println("id: " + response.getId());
        System.out.println("version: " + response.getVersion());
    }

    @Test
    void deleteDoc() throws IOException {

    }

    @Test
    void searchDoc() throws IOException {

    }
}