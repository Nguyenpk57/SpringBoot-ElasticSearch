package cn.thislx.springbootes.utils;

import cn.thislx.springbootes.bean.EsPage;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @Author: LX
 * @Description:
 * @Date: Created in 11:21 2018/11/6
 * @Modified by:
 */
@Component
public class ElasticsearchUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchUtil.class);

    @Autowired
    private TransportClient transportClient;

    private static TransportClient client;

    /**
     * @PostContruct is the annotation of the spring framework. This method is executed when the spring container is initialized.
     */
    @PostConstruct
    public void init() {
        client = this.transportClient;
    }

    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public static boolean createIndex(String index) {
        if (!isIndexExist(index)) {
            LOGGER.info("Index is not exits!");
        }
        CreateIndexResponse indexresponse = client.admin().indices().prepareCreate(index).execute().actionGet();
        LOGGER.info("Execution successful？" + indexresponse.isAcknowledged());
        return indexresponse.isAcknowledged();
    }

    /**
     * Delete index
     *
     * @param index
     * @return
     */
    public static boolean deleteIndex(String index) {
        if (!isIndexExist(index)) {
            LOGGER.info("Index is not exits!");
        }
        DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(index).execute().actionGet();
        if (dResponse.isAcknowledged()) {
            LOGGER.info("delete index " + index + "  successfully!");
        } else {
            LOGGER.info("Fail to delete index " + index);
        }
        return dResponse.isAcknowledged();
    }

    /**
     * Determine if the index exists
     *
     * @param index
     * @return
     */
    public static boolean isIndexExist(String index) {
        IndicesExistsResponse inExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        if (inExistsResponse.isExists()) {
            LOGGER.info("Index [" + index + "] is exist!");
        } else {
            LOGGER.info("Index [" + index + "] is not exist!");
        }
        return inExistsResponse.isExists();
    }

    /**
     * @Author: LX
     * @Description: Determines whether the specified type exists under inde
     * @Date: 2018/11/6 14:46
     * @Modified by:
     */
    public boolean isTypeExist(String index, String type) {
        return isIndexExist(index)
                ? client.admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists()
                : false;
    }

    /**
     * Data addition, positive definite ID
     *
     * @param jsonObject Data to add
     * @param index      Index, similar to a database
     * @param type       Type, similar to table
     * @param id         Data ID
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type, String id) {
        IndexResponse response = client.prepareIndex(index, type, id).setSource(jsonObject).get();
        LOGGER.info("addData response status:{},id:{}", response.status().getStatus(), response.getId());
        return response.getId();
    }

    /**
     * Data addition
     *
     * @param jsonObject Data to add
     * @param index      Index, similar to a database
     * @param type       Type, similar to table
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type) {
        return addData(jsonObject, index, type, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
    }

    /**
     * Delete data by ID
     *
     * @param index Index, similar to a database
     * @param type  Type, similar to table
     * @param id    Data ID
     */
    public static void deleteDataById(String index, String type, String id) {

        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();

        LOGGER.info("deleteDataById response status:{},id:{}", response.status().getStatus(), response.getId());
    }

    /**
     * Update data by ID
       *
       * @param jsonObject data to be added
       * @param index index, similar to database
       * @param type type, similar to table
       * @param id data ID
     * @return
     */
    public static void updateDataById(JSONObject jsonObject, String index, String type, String id) {

        UpdateRequest updateRequest = new UpdateRequest();

        updateRequest.index(index).type(type).id(id).doc(jsonObject);

        client.update(updateRequest);

    }

    /**
     * Get data by ID
      *
      * @param index index, similar to database
      * @param type type, similar to table
      * @param id data ID
      * @param fields The fields to be displayed, separated by commas (default is all fields)
     * @return
     */
    public static Map<String, Object> searchDataById(String index, String type, String id, String fields) {

        GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);

        if (StringUtils.isNotEmpty(fields)) {
            getRequestBuilder.setFetchSource(fields.split(","), null);
        }

        GetResponse getResponse = getRequestBuilder.execute().actionGet();

        return getResponse.getSource();
    }


    /**
      * Use word search and pagination
      * @param index index name
      * @param type type name, you can pass multiple type comma separated
      * @param startPage current page
      * @param pageSize Display number of pages per page
      * @param query query conditions
      * @param fields The fields to be displayed, separated by commas (default is all fields)
      * @param sortField sort field
      * @param highlightField highlight field
      * @return
     */
    public static EsPage searchDataPage(String index, String type, int startPage, int pageSize, QueryBuilder query, String fields, String sortField, String highlightField) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // Fields to display, separated by commas (default is all fields)
        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }

//Sort fields
        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

// Highlight (xxx = 111, aaa = 222)
        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            //highlightBuilder.preTags("<span style='color:red' >");//Set prefix
            //highlightBuilder.postTags("</span>");//Set suffix

            // Set highlight field
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

//searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        searchRequestBuilder.setQuery(query);

        // Paging application
        searchRequestBuilder.setFrom(startPage).setSize(pageSize);

        // Set whether to sort by query matching
        searchRequestBuilder.setExplain(true);

        //Printed content can be queried on Elasticsearch head and Kibana
        LOGGER.info("\n{}", searchRequestBuilder);

        // Perform a search and return search response information
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        LOGGER.debug("A total of [{}] data was found, and the number of processed data was [{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
// Parse Object
            List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);

            return new EsPage(startPage, pageSize, (int) totalHits, sourceList);
        }

        return null;

    }


    /**
     * Use word segmentation
      *
      * @param index index name
      * @param type type name, you can pass multiple type comma separated
      * @param query query conditions
      * @param size Document size limit
      * @param fields The fields to be displayed, separated by commas (default is all fields)
      * @param sortField sort field
      * @param highlightField highlight field
     * @return
     */
    public static List<Map<String, Object>> searchListData(
            String index, String type, QueryBuilder query, Integer size,
            String fields, String sortField, String highlightField) {

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }

        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            // Set highlight field
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        searchRequestBuilder.setQuery(query);

        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }
        searchRequestBuilder.setFetchSource(true);

        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        if (size != null && size > 0) {
            searchRequestBuilder.setSize(size);
        }

        //Printed content can be queried on Elasticsearch head and Kibana
        LOGGER.info("\n{}", searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        LOGGER.info("A total of [{}] data was found, and the number of processed data [{}] ", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // Parse Object
            return setSearchResponse(searchResponse, highlightField);
        }
        return null;

    }


    /**
     * Highlight result set special processing
     *
     * @param searchResponse
     * @param highlightField
     */
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
        StringBuffer stringBuffer = new StringBuffer();

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());

            if (StringUtils.isNotEmpty(highlightField)) {

                System.out.println("Traversal highlighting the result set, covering the normal result set" + searchHit.getSourceAsMap());
                Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();

                if (text != null) {
                    for (Text str : text) {
                        stringBuffer.append(str.string());
                    }
                    // traverse the highlighted result set, covering the normal result set
                    searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
                }
            }
            sourceList.add(searchHit.getSourceAsMap());
        }

        return sourceList;
    }

}
