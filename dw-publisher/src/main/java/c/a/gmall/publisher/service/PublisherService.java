package c.a.gmall.publisher.service;

import java.util.Map;

public interface PublisherService {

    Long getDauTotal(String date);

    Map<String,Long> getDauTotalHours(String date);

    Double getOrderAmount(String date);

    Map<String,Double> getOrderAmountHours(String date);

    Map getSaleDetail(String date, String keyword, int pageSize, int pageNo);
}
