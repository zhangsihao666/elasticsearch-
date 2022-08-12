package cn.itcast.hotel.pojo;

import lombok.Data;

import java.util.List;
@Data
public class Result {
    private Long total;
    private List<HotelDoc> hotels;
}
