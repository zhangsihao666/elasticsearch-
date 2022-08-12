package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.pojo.Result;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.SortField;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RequestMapping("/hotel")
@RestController
@Slf4j
public class HotelController {
    @Resource
    private RestHighLevelClient client;
//    @PostMapping("/list")
    public Result list(@RequestBody RequestParams requestParams) throws IOException {
        System.out.println(requestParams);
        Integer page = requestParams.getPage();
        Integer size = requestParams.getSize();
        String key = requestParams.getKey();
        System.out.println(key);
        SearchRequest request=new SearchRequest("hotel");
        if(key==null||"".equals(key)) {
            request.source().query(QueryBuilders.matchAllQuery());
            log.debug("sssss");
        }
        else {
            request.source().query(QueryBuilders.matchQuery("all",key));
            log.debug("ssszxxxxss");
        }
        request.source().from((page-1)*size).size(size);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        return handlerResponse(response);
    }
    @PostMapping("/list")
    public Result filter(@RequestBody RequestParams requestParams) throws IOException {
        System.out.println(requestParams);
        Integer page = requestParams.getPage();
        Integer size = requestParams.getSize();
        String key = requestParams.getKey();
        String brand = requestParams.getBrand();
        String city = requestParams.getCity();
        Integer maxPrice = requestParams.getMaxPrice();
        Integer minPrice = requestParams.getMinPrice();
        String starName = requestParams.getStarName();
        String sortBy = requestParams.getSortBy();
        String location = requestParams.getLocation();
        SearchRequest request=new SearchRequest("hotel");
        BoolQueryBuilder builder=new BoolQueryBuilder();
      myPredicate(brand,builder,"brand");
      myPredicate(city,builder,"city");
      myPredicate(starName,builder,"starName");
      if(key!=null&&!"".equals(key)){
          builder.must(QueryBuilders.matchQuery("all",key));
      }
      else {
          builder.must(QueryBuilders.matchAllQuery());
      }
      if(maxPrice!=null&&minPrice!=null) {
          System.out.println("进入价格筛选");

          builder.filter(QueryBuilders.rangeQuery("price").gte(minPrice).lte(maxPrice));
      }
      if(location!=null&&!"".equals(location)){
          request.source().sort(SortBuilders.geoDistanceSort("location",new GeoPoint(location)).
                  order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS));
      }
     request.source().query(builder);

        request.source().from((page-1)*size).size(size);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        return handlerResponse(response);

    }
    public void myPredicate(String s, BoolQueryBuilder builder,String filedName){
        if(s!=null&&!"".equals(s)){
            builder.filter(QueryBuilders.termQuery(filedName,s));
        }

    }

    public Result handlerResponse( SearchResponse response){
        Result result=new Result();
        SearchHits hits = response.getHits();
        result.setTotal(hits.getTotalHits().value);
        SearchHit[] hits1 = hits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        if(hits1!=null) {
            Arrays.stream(hits1).forEach(item -> {
                String sourceAsString = item.getSourceAsString();
                HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
                hotels.add(hotelDoc);
                Object[] sortValues = item.getSortValues();
                if(sortValues.length>0){
                    hotelDoc.setDistance(sortValues[0]);
                }
            });
        }
    result.setHotels(hotels);



        System.out.println(result);
           return result;
    }

}
