package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.impl.HotelService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.util.BeanUtil;
import net.minidev.json.JSONUtil;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeanUtils;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import javax.naming.directory.SearchResult;
import javax.swing.text.Highlighter;
import java.io.IOException;
import java.util.*;

import static cn.itcast.hotel.DSL.hotel.*;

@SpringBootTest
class HotelDemoApplicationTests {
private RestHighLevelClient client;
@Resource
private HotelService hotelService;
   @BeforeEach
    void contextLoads() {
       RestHighLevelClient client=new RestHighLevelClient(RestClient.builder(HttpHost.create("192.168.18.133:9200")));
        this.client=client;
    }
    @Test
    void createIndex() throws IOException {
       //创建请求对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        request.source(HOTEL_createIndexDSL, XContentType.JSON);
        //发送创建索引请求
        client.indices().create(request,RequestOptions.DEFAULT);

    }
    @Test
    void deleteIndex() throws IOException {
        //创建请求对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
//        request.source(HOTEL_createIndexDSL, XContentType.JSON);
        //发送创建索引请求
        client.indices().delete(request,RequestOptions.DEFAULT);

    }
    @Test
    void updateIndex() throws IOException {
      PutMappingRequest request=new PutMappingRequest("hotel");
      request.source(HOTEL_POST,XContentType.JSON);
      client.indices().putMapping(request,RequestOptions.DEFAULT);

    }
    @Test
   void isExist() throws IOException {
        GetIndexRequest request=new GetIndexRequest("hotel");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);

    }
    @Test
    void insertDoc() throws IOException {
        Hotel hotel = hotelService.getById(36934);
        HotelDoc hotelDoc = new HotelDoc(hotel);
        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        client.index(request,RequestOptions.DEFAULT);
    }
    @Test
    void queryDoc() throws IOException {
        GetRequest request = new GetRequest("hotel","36934");
        GetResponse response=client.get(request,RequestOptions.DEFAULT);
        String sourceAsString = response.getSourceAsString();
        System.out.println(sourceAsString);
        HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
        System.out.println(hotelDoc);
    }
    @Test
    void updateDoc() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel","394617");
        request.doc("isAD","true");
        client.update(request,RequestOptions.DEFAULT);
    }
    @Test
    void deleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest("hotel").id("36934");
        client.delete(request,RequestOptions.DEFAULT);
    }
    @Test
    void mutipInsert(){

        List<Hotel> list = hotelService.list();
        list.stream().forEach(item->{
            HotelDoc hotelDoc = new HotelDoc(item);
            IndexRequest request=new IndexRequest("hotel").id(hotelDoc.getId().toString());
            request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
            try {
                client.index(request,RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
    @Test
    void TestMatchAll() throws IOException {
        SearchRequest request=new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        SearchResponse response=client.search(request,RequestOptions.DEFAULT);
       //解析结果
        parseResponse(response);
    }
    @Test
    void TestMatch() throws IOException {
        SearchRequest request=new SearchRequest("hotel");
//        request.source().query(QueryBuilders.matchAllQuery());
        request.source().query(QueryBuilders.matchQuery("isAD","true"));
        SearchResponse response=client.search(request,RequestOptions.DEFAULT);
        //解析结果
        parseResponse(response);
    }
    @Test
    void TestMultiMatch() throws IOException {
        SearchRequest request=new SearchRequest("hotel");

        request.source().query(QueryBuilders.multiMatchQuery("如家","name","brand"));
        SearchResponse response=client.search(request,RequestOptions.DEFAULT);
        parseResponse(response);
    }
    @Test
    void TestTermSearch() throws IOException {
        SearchRequest request=new SearchRequest("hotel");

        request.source().query(QueryBuilders.termQuery("starName","二钻"));
        SearchResponse response=client.search(request,RequestOptions.DEFAULT);
        parseResponse(response);
    }
    @Test
    void TestRangeSearch() throws IOException {
        SearchRequest request=new SearchRequest("hotel");
        request.source().query(QueryBuilders.rangeQuery("price").gte(100).lte(300));
        SearchResponse response=client.search(request,RequestOptions.DEFAULT);
        parseResponse(response);
    }
    @Test
    void boolSearch() throws IOException {
        SearchRequest request=new SearchRequest("hotel");
        BoolQueryBuilder builder=new BoolQueryBuilder();
        builder.must(QueryBuilders.matchQuery("brand","如家"));
        builder.mustNot(QueryBuilders.rangeQuery("price").gte(1000));
        builder.filter(QueryBuilders.termQuery("city","上海"));
        request.source().query(builder);
        SearchResponse response=client.search(request,RequestOptions.DEFAULT);
        parseResponse(response);
    }
    @Test
    void sortAndPage() throws IOException {
       SearchRequest request=new SearchRequest("hotel");
       request.source().size(10).from(20);
       request.source().sort("price", SortOrder.DESC);
       request.source().query(QueryBuilders.matchAllQuery());
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        parseResponse(response);
    }

    @Test
    void highLight() throws IOException {
        SearchRequest request=new SearchRequest("hotel");
      request.source().query(QueryBuilders.matchQuery("all","7天"));
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("name").requireFieldMatch(false);
        request.source().highlighter(highlightBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        Arrays.stream(hits).forEach(item->{
            String sourceAsString = item.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            Map<String, HighlightField> highlightFields = item.getHighlightFields();
            HighlightField name = highlightFields.get("name");
            Text[] fragments = name.getFragments();
            if(name!=null)
            {
                hotelDoc.setName(fragments[0].toString());
            }
            System.out.println(hotelDoc);
        });

    }
    @Test
    void test02(){
       List<Integer>  list=new ArrayList<>();

       list.stream().forEach(item->{

               System.out.println(item);

       });
    }
    private void parseResponse(SearchResponse response) {
        //解析结果
        SearchHits hits = response.getHits();
        long value = hits.getTotalHits().value;//查询数据条数
        System.out.println(value);
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hits2:hits1){
            System.out.println(hits2);
            //每条数据的hits（记录）
        }
    }
}
