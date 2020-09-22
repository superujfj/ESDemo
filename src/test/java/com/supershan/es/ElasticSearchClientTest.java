package com.supershan.es;

import org.elasticsearch.action.index.IndexResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("My first test case")
class ElasticSearchClientTest {
    ElasticSearchClient esClient = null;


    @BeforeEach
    void setUp() {
        esClient = new ElasticSearchClient(new String[]{"127.0.0.1:9200"});
    }

    @AfterEach
    void tearDown() throws IOException {
        esClient.close();
    }

    /**
     * 创建索引
     */
    @Test
    void createIndex() throws IOException {
        String index = "users";
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("id", "1");
        jsonMap.put("name", "A");
        jsonMap.put("subject", "Chinese");
        jsonMap.put("score", 90);
        jsonMap.put("password", "A2020");

        IndexResponse response = esClient.execIndex(index, jsonMap);
        System.out.println("index: " + response.getIndex());
        System.out.println("id: " + response.getId());
        System.out.println("version: " + response.getVersion());
    }

    @Test
    void getDoc() throws IOException {

    }

    @Test
    void putDoc() throws IOException {

    }

    @Test
    void deleteDoc() throws IOException {

    }
}