package com.supershan.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mac
 */
public class ElasticSearchClient {
    private String[] hostsAndPorts;
    private RestHighLevelClient client;

    public RestHighLevelClient getClient() {
        return this.client;
    }

    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }

    public ElasticSearchClient(String[] hostsAndPorts) {
        this.hostsAndPorts = hostsAndPorts;

        List<HttpHost> httpHosts = new ArrayList<HttpHost>();
        if (null != hostsAndPorts && hostsAndPorts.length > 0) {
            for (String hostsAndPort : hostsAndPorts) {
                String[] hp = hostsAndPort.split(":");
                httpHosts.add(new HttpHost(hp[0], Integer.valueOf(hp[1]), "http"));
            }
            this.client = new RestHighLevelClient(
                    RestClient.builder(httpHosts.toArray(new HttpHost[0])));
        } else {
            this.client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost("127.0.0.1", 9200, "http")));
        }
    }

    public void close() throws IOException {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private IndexRequest getIndexRequest(String index) {
        IndexRequest indexRequest = null;
        if (null == index) {
            throw new ElasticsearchException("index must not be null");
        } else {
            indexRequest = new IndexRequest(index);
        }
        return indexRequest;
    }


    /**
     * 同步执行索引
     *
     * @param index
     * @param dataMap
     * @throws IOException
     */
    public IndexResponse execIndex(String index, Map<String, Object> dataMap) throws IOException {
        return getClient().index(getIndexRequest(index).source(dataMap), RequestOptions.DEFAULT);
    }

    /**
     * 异步执行
     *
     * @param index
     * @param dataMap
     * @param indexResponseActionListener
     * @throws IOException
     */
    public void asyncExecIndex(String index, Map<String, Object> dataMap, ActionListener<IndexResponse> indexResponseActionListener) throws IOException {
        getClient().indexAsync(getIndexRequest(index).source(dataMap), RequestOptions.DEFAULT, indexResponseActionListener);
    }

    /**
     * @param index
     * @param docId
     * @param includes  返回需要包含的字段，可以传入空
     * @param excludes  返回需要不包含的字段，可以传入为空
     * @param excludes  version
     * @param excludes  versionType
     * @return
     * @throws IOException
     */

    public GetResponse getResponse(String index, String docId, String[] includes, String[] excludes, Integer version, VersionType versionType) throws IOException {
        if (null == includes || includes.length == 0) {
            includes = Strings.EMPTY_ARRAY;
        }
        if (null == excludes || excludes.length == 0) {
            excludes = Strings.EMPTY_ARRAY;
        }
        GetRequest getRequest = new GetRequest(index, docId);
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.realtime(true);
        if (null != version) {
            getRequest.version(version);
        }
        if (null != versionType) {
            getRequest.versionType(versionType);
        }
        return getClient().get(getRequest.fetchSourceContext(fetchSourceContext), RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param docId
     * @param includes
     * @param excludes
     * @return
     * @throws IOException
     */

    public GetResponse getResponse(String index, String docId, String[] includes, String[] excludes) throws IOException {
        return getResponse(index, docId, includes, excludes, null, null);
    }

    /**
     * @param index
     * @param docId
     * @return
     * @throws IOException
     */
    public GetResponse getResponse(String index, String docId) throws IOException {
        GetRequest getRequest = new GetRequest(index, docId);
        return getClient().get(getRequest, RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param docId
     * @return
     * @throws IOException
     */
    public Boolean existDoc(String index, String docId) throws IOException {
        GetRequest getRequest = new GetRequest(index, docId);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        return getClient().exists(getRequest, RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param docId
     * @param timeValue
     * @param refreshPolicy
     * @param version
     * @param versionType
     * @return
     * @throws IOException
     */
    public DeleteResponse deleteDoc(String index, String docId, TimeValue timeValue, WriteRequest.RefreshPolicy refreshPolicy, Integer version, VersionType versionType) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, docId);
        if (null != timeValue) {
            deleteRequest.timeout(timeValue);
        }
        if (null != refreshPolicy) {
            deleteRequest.setRefreshPolicy(refreshPolicy);
        }
        if (null != version) {
            deleteRequest.version(version);
        }
        if (null != versionType) {
            deleteRequest.versionType(versionType);
        }
        return getClient().delete(deleteRequest, RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param docId
     * @return
     * @throws IOException
     */
    public DeleteResponse deleteDoc(String index, String docId) throws IOException {
        return deleteDoc(index, docId, null, null, null, null);
    }

    /**
     * @param index
     * @param docId
     * @param dataMap
     * @param timeValue
     * @param refreshPolicy
     * @param version
     * @param versionType
     * @param docAsUpsert
     * @param includes
     * @param excludes
     * @return
     * @throws IOException
     */
    public UpdateResponse updateDoc(String index,  String docId, Map<String, Object> dataMap, TimeValue timeValue, WriteRequest.RefreshPolicy refreshPolicy, Integer version, VersionType versionType, Boolean docAsUpsert, String[] includes, String[] excludes) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(index, docId);
        updateRequest.doc(dataMap);
        if (null != timeValue) {
            updateRequest.timeout(timeValue);
        }
        if (null != refreshPolicy) {
            updateRequest.setRefreshPolicy(refreshPolicy);
        }
        if (null != version) {
            updateRequest.version(version);
        }
        if (null != versionType) {
            updateRequest.versionType(versionType);
        }
        updateRequest.docAsUpsert(docAsUpsert);
        //冲突时重试的次数
        updateRequest.retryOnConflict(3);
        if (null == includes && null == excludes) {
            return getClient().update(updateRequest, RequestOptions.DEFAULT);
        } else {
            if (null == includes || includes.length == 0) {
                includes = Strings.EMPTY_ARRAY;
            }
            if (null == excludes || excludes.length == 0) {
                excludes = Strings.EMPTY_ARRAY;
            }
            return getClient().update(updateRequest.fetchSource(new FetchSourceContext(true, includes, excludes)), RequestOptions.DEFAULT);
        }
    }

    /**
     * 更新时不存在就插入
     *
     * @param index
     * @param docId
     * @param dataMap
     * @return
     * @throws IOException
     */
    public UpdateResponse upDdateocAsUpsert(String index, String docId, Map<String, Object> dataMap) throws IOException {
        return updateDoc(index, docId, dataMap, null, null, null, null, true, null, null);
    }

    /**
     * 存在才更新
     *
     * @param index
     * @param docId
     * @param dataMap
     * @return
     * @throws IOException
     */
    public UpdateResponse updateDoc(String index, String docId, Map<String, Object> dataMap) throws IOException {
        return updateDoc(index, docId, dataMap, null, null, null, null, false, null, null);
    }

    /**
     * 批量操作
     *
     * @param indexBeanList
     * @param timeValue
     * @param refreshPolicy
     * @return
     * @throws IOException
     */
    public BulkResponse bulkRequest(List<IndexBean> indexBeanList, TimeValue timeValue, WriteRequest.RefreshPolicy refreshPolicy) throws IOException {
        BulkRequest bulkRequest = getBulkRequest(indexBeanList);
        if (null != timeValue) {
            bulkRequest.timeout(timeValue);
        }
        if (null != refreshPolicy) {
            bulkRequest.setRefreshPolicy(refreshPolicy);
        }
        return getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    private BulkRequest getBulkRequest(List<IndexBean> indexBeanList) {
        BulkRequest bulkRequest = new BulkRequest();
        indexBeanList.forEach(indexBean -> {
            if ("1".equals(indexBean.getOperateType())) {
                bulkRequest.add(new IndexRequest(indexBean.getIndex()));
            } else if ("2".equals(indexBean.getOperateType())) {
                if ((null != indexBean.getDocId())) {
                    throw new ElasticsearchException("update action docId must not be null");
                }
                bulkRequest.add(new UpdateRequest(indexBean.getIndex(), indexBean.getDocId()));
            } else if ("3".equals(indexBean.getOperateType())) {
                if ((null != indexBean.getDocId())) {
                    throw new ElasticsearchException("delete action docId must not be null");
                }
                bulkRequest.add(new DeleteRequest(indexBean.getIndex(), indexBean.getDocId()));
            } else {
                throw new ElasticsearchException("OperateType" + indexBean.getOperateType() + "is not support");
            }
        });
        return bulkRequest;
    }

    /**
     * 批量操作
     *
     * @param indexBeanList
     * @return
     */
    public BulkResponse bulkRequest(List<IndexBean> indexBeanList) throws IOException {
        return bulkRequest(indexBeanList, null, null);
    }

    /**
     * 批量异步操作
     *
     * @param indexBeanList
     * @param bulkResponseActionListener
     */

    public void AsyncBulkRequest(List<IndexBean> indexBeanList, ActionListener<BulkResponse> bulkResponseActionListener) {
        getClient().bulkAsync(getBulkRequest(indexBeanList), RequestOptions.DEFAULT, bulkResponseActionListener);
    }


    private SearchRequest getSearchRequest(String index) {
        SearchRequest searchRequest;
        if (null == index) {
            throw new ElasticsearchException("index name must not be null");
        }
        else {
            searchRequest = new SearchRequest(index);
        }
        return searchRequest;
    }

    /**
     * @param index
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index) throws IOException {
        return getClient().search(getSearchRequest(index), RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param from
     * @param size
     * @param termQueryBuilder
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index, Integer from, Integer size, TermQueryBuilder termQueryBuilder) throws IOException {
        return getClient().search(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, termQueryBuilder, null, null, null)), RequestOptions.DEFAULT);
    }

    private SearchSourceBuilder getSearchSourceBuilder(String index, Integer from, Integer size, TermQueryBuilder termQueryBuilder, String sortField, SortBuilder sortBuilder, Boolean fetchSource) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (null != termQueryBuilder) {
            searchSourceBuilder.query(termQueryBuilder);
        }
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);
        if (null != sortField) {
            searchSourceBuilder.sort(sortField);
        }
        if (null != sortBuilder) {
            searchSourceBuilder.sort(sortBuilder);
        }
        //设置超时时间
        searchSourceBuilder.timeout(new TimeValue(120, TimeUnit.SECONDS));
        if (null != fetchSource) {
            searchSourceBuilder.fetchSource(fetchSource);
        }
        return searchSourceBuilder;
    }

    /**
     * @param index
     * @param from
     * @param size
     * @param termQueryBuilder
     * @param matchQueryBuilder
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index, Integer from, Integer size, TermQueryBuilder termQueryBuilder, MatchQueryBuilder matchQueryBuilder) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        return getClient().search(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, termQueryBuilder, null, null, null).query(matchQueryBuilder)), RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param from
     * @param size
     * @param matchQueryBuilder
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index, Integer from, Integer size, MatchQueryBuilder matchQueryBuilder) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        return getClient().search(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, null, null, null, null).query(matchQueryBuilder)), RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param from
     * @param size
     * @param matchQueryBuilder
     * @param sortField
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index, Integer from, Integer size, MatchQueryBuilder matchQueryBuilder, String sortField) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        return getClient().search(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, null, sortField, null, null).query(matchQueryBuilder)), RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param from
     * @param size
     * @param matchQueryBuilder
     * @param sortField
     * @param fetchSource
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index, Integer from, Integer size, MatchQueryBuilder matchQueryBuilder, String sortField, Boolean fetchSource) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        return getClient().search(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, null, sortField, null, fetchSource).query(matchQueryBuilder)), RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param from
     * @param size
     * @param matchQueryBuilder
     * @param sortBuilder
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index, Integer from, Integer size, MatchQueryBuilder matchQueryBuilder, SortBuilder sortBuilder) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        return getClient().search(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, null, null, sortBuilder, null).query(matchQueryBuilder)), RequestOptions.DEFAULT);
    }

    /**
     * 支持排序
     *
     * @param index
     * @param from
     * @param size
     * @param termQueryBuilder
     * @param matchQueryBuilder
     * @param sortField
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index, Integer from, Integer size, TermQueryBuilder termQueryBuilder, MatchQueryBuilder matchQueryBuilder, String sortField) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        return getClient().search(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, termQueryBuilder, sortField, null, null).query(matchQueryBuilder)), RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param from
     * @param size
     * @param termQueryBuilder
     * @param matchQueryBuilder
     * @param sortBuilder
     * @param fetchSource       开关
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index, Integer from, Integer size, TermQueryBuilder termQueryBuilder, MatchQueryBuilder matchQueryBuilder, SortBuilder sortBuilder, Boolean fetchSource) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        return getClient().search(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, termQueryBuilder, null, sortBuilder, fetchSource).query(matchQueryBuilder)), RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param from
     * @param size
     * @param termQueryBuilder
     * @param matchQueryBuilder
     * @param sortBuilder
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index, Integer from, Integer size, TermQueryBuilder termQueryBuilder, MatchQueryBuilder matchQueryBuilder, SortBuilder sortBuilder) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        return getClient().search(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, termQueryBuilder, null, sortBuilder, null).query(matchQueryBuilder)), RequestOptions.DEFAULT);
    }

    /**
     * @param index
     * @param from
     * @param size
     * @param termQueryBuilder
     * @param matchQueryBuilder
     * @param sortField
     * @param fetchSource
     * @return
     * @throws IOException
     */
    public SearchResponse searchResponse(String index, Integer from, Integer size, TermQueryBuilder termQueryBuilder, MatchQueryBuilder matchQueryBuilder, String sortField, Boolean fetchSource) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        return getClient().search(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, termQueryBuilder, sortField, null, fetchSource).query(matchQueryBuilder)), RequestOptions.DEFAULT);
    }

    /**
     * 异步操作
     *
     * @param index
     * @param from
     * @param size
     * @param termQueryBuilder
     * @param matchQueryBuilder
     * @param sortBuilder
     * @param listener
     * @throws IOException
     */

    public void asyncSearchRequest(String index, Integer from, Integer size, TermQueryBuilder termQueryBuilder, MatchQueryBuilder matchQueryBuilder, SortBuilder sortBuilder, ActionListener<SearchResponse> listener) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        getClient().searchAsync(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, termQueryBuilder, null, sortBuilder, null).query(matchQueryBuilder)), RequestOptions.DEFAULT, listener);
    }

    /**
     * 异步操作
     *
     * @param index
     * @param from
     * @param size
     * @param termQueryBuilder
     * @param matchQueryBuilder
     * @param sortField
     * @param listener
     * @throws IOException
     */
    public void asyncSearchRequest(String index, Integer from, Integer size, TermQueryBuilder termQueryBuilder, MatchQueryBuilder matchQueryBuilder, String sortField, ActionListener<SearchResponse> listener) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder is null");
        }
        getClient().searchAsync(getSearchRequest(index).source(getSearchSourceBuilder(index, from, size, termQueryBuilder, sortField, null, null).query(matchQueryBuilder)), RequestOptions.DEFAULT, listener);
    }
}