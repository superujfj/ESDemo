package com.supershan.es;

import net.sf.json.JSONObject;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author mac
 */
public class TransportClientTest {
    private static TransportClient client;

    static {
        try {
            client = new ClientUtil().createClient();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建索引，普通格式
     *
     * @throws Exception
     */
    public static void createEmployee() throws Exception {
        IndexResponse response = client.prepareIndex("student", "doc", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "jack")
                        .field("age", 27)
                        .field("position", "technique")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 10000)
                        .endObject())
                .get();
        System.out.println("创建成功！");
    }

    public static void FindIndex() throws Exception {
        GetResponse getResponse = client.prepareGet("student", "doc", "1").get();
        System.out.println(getResponse.getSourceAsString());
    }

    /**
     * 创建索引，JSON
     *
     * @throws IOException
     */
    public static void CreateJsonIndex() throws IOException {
        JSONObject json = new JSONObject();
        json.put("user", "小明");
        json.put("title", "Java Engineer");
        json.put("desc", "web 开发");
        IndexResponse response = client.prepareIndex("studentjson", "doc", "1")
                .setSource(json, XContentType.JSON)
                .get();
        String _index = response.getIndex();
        System.out.println(_index);
    }

    /**
     * elasticsearch批量导入
     */
    public static void BulkCreateIndex() {
        BulkRequestBuilder builder = client.prepareBulk();
        for (int i = 0; i < 10; i++) {
            HashMap<String, Object> map = new HashMap<String, Object>(16);
            map.put("recordtime", "11");
            map.put("area", "22");
            map.put("usertype", "33");
            map.put("count", 44);
            builder.add(client.prepareIndex("bulktest", "_doc").setSource(map));
            //每10000条提交一次
            if (i % 10000 == 0) {
                builder.execute().actionGet();
                builder = client.prepareBulk();
            }
        }

        if (builder.numberOfActions() != 0) {
            builder.execute().actionGet();
        }
    }

    /**
     * 批量导出
     */
    public static void OutData() throws IOException {
        SearchResponse response = client.prepareSearch("bulktest").setTypes("_doc")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(1).setScroll(new TimeValue(600000))
                .setSearchType(SearchType.DEFAULT).execute().actionGet();
        // setScroll(new TimeValue(600000)) 设置滚动的时间
        String scrollid = response.getScrollId();
        //把导出的结果以JSON的格式写到文件里

        for (SearchHit searchHit : response.getHits().getHits()) {
            String json = searchHit.getSourceAsString();
            putData(json);
        }
        //每次返回数据10000条。一直循环查询知道所有的数据都被查询出来
        while (true) {
            SearchResponse response2 = client.prepareSearchScroll(scrollid).setScroll(new TimeValue(1000000))
                    .execute().actionGet();
            SearchHits searchHit = response2.getHits();
            //再次查询不到数据时跳出循环
            if (searchHit.getHits().length == 0) {
                break;
            }
            System.out.println("查询数量 ：" + searchHit.getHits().length);
            for (int i = 0; i < searchHit.getHits().length; i++) {
                String json = searchHit.getHits()[i].getSourceAsString();
                putData(json);
            }
        }

        System.out.println("查询结束");
    }

    public static void putData(String json) throws IOException {
        String str = json + "\n";
        //写入本地文件
        String fileTxt = "/Users/mac/IDEA/workspace/ESDemo/data/data.txt";
        File file = new File(fileTxt);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        if (!file.exists()) {
            file.createNewFile();
            FileWriter fw = new FileWriter(file, true);
            BufferedWriter bw = new BufferedWriter(fw);
            System.out.println("写入完成啦啊");
            bw.write(String.valueOf(str));
            bw.flush();
            bw.close();
            fw.close();
        } else {
            FileWriter fw = new FileWriter(file, true);
            BufferedWriter bw = new BufferedWriter(fw);
            System.out.println("追加写入完成啦啦");
            bw.write(String.valueOf(str));
            bw.flush();
            bw.close();
            fw.close();
        }
    }

    /**
     * 创建索引，并给某些字段指定ik分词器，以后向该索引中查询时，就会用ik分词
     */
    public static void CreateIndexIkTest() throws Exception {
        //创建映射
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                //title:字段名，  type:文本类型       analyzer ：分词器类型
                //该字段添加的内容，查询时将会使用ik_smart分词
                .startObject("title").field("type", "text").field("analyzer", "ik_smart").endObject()
                .startObject("content").field("type", "text").field("analyzer", "ik_max_word").endObject()
                .endObject()
                .endObject();

        //index：索引名   type：类型名（可以自己定义）
        PutMappingRequest putmap = Requests.putMappingRequest("my_index").type("_doc").source(mapping);
        //创建索引
        client.admin().indices().prepareCreate("my_index").execute().actionGet();
        //为索引添加映射
        client.admin().indices().putMapping(putmap).actionGet();

        //调用下面的方法为创建的索引添加内容
        CreateIndex1();
    }

    /**
     * 这个方法是为上一步创建的索引中添加内容，包括id，id不能重复
     */

    public static void CreateIndex1() throws IOException {
        //索引，类型，id
        IndexResponse response = client.prepareIndex("my_index", "_doc", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        //字段，值
                        .field("title", "title")
                        .field("content", "content")
                        .endObject()
                ).get();
    }

    /**
     * 更新索引，更新刚才创建的索引，如果id相同将会覆盖掉刚才的内容
     */

    public static void UpdateIndex() throws Exception {
        //每次添加id应该不同，相当于数据表中的主键，相同的话将会进行覆盖
        UpdateResponse response = client.update(new UpdateRequest("my_index", "_doc", "1")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "中华人民共和国国歌,国歌是最好听的歌")
                        .field("content", "中华人民共和国国歌,国歌是最好听的歌")
                        .endObject()
                )).get();
    }

    /**再插入一条数据
     *
     * @throws IOException
     */
    public static void createIndex2() throws IOException {
        IndexResponse response = client.prepareIndex("my_index", "_doc", "2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("title", "中华民族是伟大的民族")
                        .field("content", "中华民族是伟大的民族")
                        .endObject()
                ).get();
    }

    /**
     * 下面使用index索引下的2个document进行查询
     */
    public static void Search() {
        //指定多个索引
        SearchResponse response1 = client.prepareSearch("my_index")
                //指定类型
                .setTypes("type")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                // Query
                .setQuery(QueryBuilders.matchQuery("title", "中华人民共和国国歌"))
                .setFrom(0).setSize(60).setExplain(true)
                .get();
        //命中个数
        TotalHits totalHits1 = response1.getHits().getTotalHits();
        System.out.println("response1=======" + totalHits1);
        //指定多个索引
        SearchResponse response2 = client.prepareSearch("my_index")
                //指定类型
                .setTypes("_doc")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                // Query
                .setQuery(QueryBuilders.matchQuery("content", "中华人民共和国国歌"))
                .setFrom(0).setSize(60).setExplain(true)
                .get();
        //命中个数
        TotalHits totalHits2 = response2.getHits().getTotalHits();
        System.out.println("response2=========" + totalHits2);
    }

    /**
     * 使用过滤器查询，实现分页查询
     */
    public static void queryByFilter() {

        // 查询groupname为"压力测试"的数据
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "sea");

        SearchResponse response = client.prepareSearch().setIndices("movie_index").setQuery(queryBuilder).get();
        TotalHits totalHits = response.getHits().getTotalHits();

        System.out.println(totalHits);
        int size = 100;
        List<String> retList = new ArrayList<String>();

        response = client.prepareSearch().setIndices("movie_index").setScroll(TimeValue.timeValueMinutes(5)).setSize(100).setQuery(queryBuilder).get();
        SearchHits searchHits = response.getHits();
        for (SearchHit hit : searchHits) {
            retList.add(hit.getSourceAsString());
            System.out.println("doc:内容'\n'" + hit.getSourceAsString());
        }
    }

    /**
     * GET操作
     */
    public static void get() {
        GetResponse response = client.prepareGet("movie_index", "movie", "1").get();
        Map<String, Object> source = response.getSource();
        Set<String> strings = source.keySet();
        Iterator<String> iterator = strings.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            System.out.println(key + ":" + source.get(key));

        }
    }

    public static void main(String[] args) throws Exception {
        //创建索引
        //createEmployee();

        //根据inde，type，id查询一个document的data
        //FindIndex();


        //CreateJsonIndex();

        //批量导入
        //BulkCreateIndex();

        //批量导出
        //OutData();

        //创建带ik分词的index
        //CreateIndexIkTest();

        //更新索引
        //UpdateIndex();

        //createIndex2();

        //Search();

        //get();

        queryByFilter();
    }

}
