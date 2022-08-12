package cn.itcast.hotel.pojo;

import lombok.Data;

@Data
public class RequestParams {
    private String key;
    private Integer page;
    private Integer size;
    private String orderBy;
    private Integer maxPrice;
    private Integer minPrice;
    private String  city;
    private String brand;
   private String sortBy;
   private String starName;
   private String location;


}
