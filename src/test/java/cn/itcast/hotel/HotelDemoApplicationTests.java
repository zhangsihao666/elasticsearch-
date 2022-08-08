package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.impl.HotelService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.util.BeanUtil;
import net.minidev.json.JSONUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeanUtils;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.itcast.hotel.DSL.hotel.HOTEL_createIndexDSL;
import static cn.itcast.hotel.DSL.hotel.HOTEL_createIndexDSL;

@SpringBootTest
class HotelDemoApplicationTests {
private RestHighLevelClient client;
@Resource
private HotelService hotelService;
   @BeforeEach
    void contextLoads() {
       RestHighLevelClient client=new RestHighLevelClient(RestClient.builder(HttpHost.create("192.168.18.132:9200")));
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
        UpdateRequest request = new UpdateRequest("hotel","36934");
        request.doc("name","老八酒店");
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
}
