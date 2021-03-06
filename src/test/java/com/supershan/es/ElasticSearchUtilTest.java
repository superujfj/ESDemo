package com.supershan.es;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
    void createIndex() {
        String index = "users";
        System.out.println(esUtil.createIndex(index) ? "创建索引成功" : "创建索引失败");
    }

    /**
     *
     *
     */
    @Test
    void deleteIndex() {
        String index = "users";
        System.out.println(esUtil.deleteIndex(index) ? "删除索引成功" : "删除索引失败");
    }


    @Test
    void existsIndex() throws IOException {
        String index = "users";
        System.out.println(esUtil.existsIndex(index) ? "索引已存在" : "索引不存在");
    }

    /**
     * 创建Doc
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

            System.out.println("文档创建成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void getDoc() throws IOException {
        String index = "users";
        String docId = "1";

        GetResponse response = esUtil.getDoc(index, docId);

        Map<String, Object> source = response.getSource();
        Set<String> strings = source.keySet();
        for (String key : strings) {
            System.out.println(key + ":" + source.get(key));
        }
    }

    @Test
    void existsDoc() throws IOException {
        String index = "users";
        String docId = "1";

        System.out.println(esUtil.existsDoc(index, docId) ? "文档已存在" : "文档不存在");
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
        String index = "users";
        String docId = "1";

        DeleteResponse deleteResponse = esUtil.deleteDoc(index, docId);
        System.out.println("index: " + deleteResponse.getIndex());
        System.out.println("id: " + deleteResponse.getId());
    }

    @Test
    void bulkDocs() throws IOException {
        String index = "users";

        Map<String, Object> jsonMap1 = new HashMap<>();
        jsonMap1.put("id", "1");
        jsonMap1.put("name", "A");
        jsonMap1.put("subject", "English lesson");
        jsonMap1.put("score", 90);
        jsonMap1.put("password", "A2020");
        jsonMap1.put("birthday", "2020-01-01");

        Map<String, Object> jsonMap2 = new HashMap<>();
        jsonMap2.put("id", "2");
        jsonMap2.put("name", "B");
        jsonMap2.put("subject", "English lesson");
        jsonMap2.put("score", 100);
        jsonMap2.put("password", "B2020");
        jsonMap2.put("birthday", "2020-06-01");

        Map<String, Object> jsonMap3 = new HashMap<>();
        jsonMap3.put("id", "3");
        jsonMap3.put("name", "C");
        jsonMap3.put("subject", "Chinese lesson");
        jsonMap3.put("score", 80);
        jsonMap3.put("password", "C2020");
        jsonMap3.put("birthday", "2020-07-01");

        ArrayList list = new ArrayList();

        list.add(jsonMap1);
        list.add(jsonMap2);
        list.add(jsonMap3);

        BulkResponse response = esUtil.bulkDocs(index, list);

        for (BulkItemResponse itemResponse : response) {
            System.out.println("id: " + itemResponse.getId());
        }
    }


    @Test
    void searchByIndex() throws IOException {
        String index = "users";
        SearchResponse response = esUtil.searchResponse(index);

        System.out.println("response: " + format(response.toString()));
    }

    @Test
    void matchSearch() throws IOException {
        String index = "users";
        SearchRequest searchRequest = new SearchRequest(index);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("name", "C");
        sourceBuilder.query(matchQueryBuilder); // 设置搜索，可以是任何类型的 QueryBuilder
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);

        SearchResponse response = esUtil.searchResponse(searchRequest);
        System.out.println("response: " + format(response.toString()));
    }

    @Test
    void fuzzySearch() throws IOException {
        String index = "users";
        SearchRequest searchRequest = new SearchRequest(index);
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("subject", "lesson");
        matchQueryBuilder.fuzziness();
        boolBuilder.must(matchQueryBuilder);

        sourceBuilder.query(boolBuilder);

        searchRequest.source(sourceBuilder);
        SearchResponse response = esUtil.searchResponse(searchRequest);
        System.out.println("response: " + format(response.toString()));
    }

    @Test
    void sortSearch() throws IOException {
        String index = "users";
        SearchRequest searchRequest = new SearchRequest(index);
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("birthday");
        rangeQueryBuilder.gte("2020-01-01"); //开始时间
        rangeQueryBuilder.lte("2020-06-30"); //结束时间
        boolBuilder.must(rangeQueryBuilder);

        sourceBuilder.sort(new FieldSortBuilder("name").order(SortOrder.DESC));
        sourceBuilder.query(boolBuilder);

        searchRequest.source(sourceBuilder);
        SearchResponse response = esUtil.searchResponse(searchRequest);
        System.out.println("response: " + format(response.toString()));
    }

    public static String format(String jsonStr) {
        int level = 0;
        StringBuilder jsonForMatStr = new StringBuilder();
        for (int i = 0; i < jsonStr.length(); i++) {
            char c = jsonStr.charAt(i);
            if (level > 0 && '\n' == jsonForMatStr.charAt(jsonForMatStr.length() - 1)) {
                jsonForMatStr.append(getLevelStr(level));
            }
            switch (c) {
                case '{':
                case '[':
                    jsonForMatStr.append(c).append("\n");
                    level++;
                    break;
                case ',':
                    jsonForMatStr.append(c).append("\n");
                    break;
                case '}':
                case ']':
                    jsonForMatStr.append("\n");
                    level--;
                    jsonForMatStr.append(getLevelStr(level));
                    jsonForMatStr.append(c);
                    break;
                default:
                    jsonForMatStr.append(c);
                    break;
            }
        }

        return jsonForMatStr.toString();

    }

    private static String getLevelStr(int level) {
        StringBuilder levelStr = new StringBuilder();
        for (int levelI = 0; levelI < level; levelI++) {
            levelStr.append("\t");
        }
        return levelStr.toString();
    }
}