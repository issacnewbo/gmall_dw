package c.a.gmall.publisher.service.impl;

import c.a.gmall.constant.GmallConstants;
import c.a.gmall.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    JestClient jestClient;

//    @Autowired
//    DauMapper dauMapper;
//
//    @Autowired
//    OrderMapper orderMapper;
//
//    @Override
//    public Long getDauTotal(String date) {
//        return dauMapper.selectDauTotal(date);
//    }
//
//    @Override
//    public Map<String, Long> getDauTotalHours(String date) {
//        //变换格式 [{"LH":"11","CT":489},{"LH":"12","CT":123},{"LH":"13","CT":4343}]
//        //===》 {"11":383,"12":123,"17":88,"19":200 }
//        List<Map> dauListMap = dauMapper.selectDauTotalHours(date);
//        Map<String, Long> dauMap = new HashMap<>();
//        for (Map map : dauListMap) {
//            String lh = (String) map.get("LH");
//            Long ct = (Long) map.get("CT");
//            dauMap.put(lh, ct);
//        }
//        return dauMap;
//    }
//
//    @Override
//    public Double getOrderAmount(String date) {
//        return orderMapper.selectOrderAmount(date);
//    }
//
//    @Override
//    public Map<String, Double> getOrderAmountHours(String date) {
//        //变换格式 [{"C_HOUR":"11","AMOUNT":489.0},{"C_HOUR":"12","AMOUNT":223.0}]
//        //===》 {"11":489.0,"12":223.0 }
//        Map<String, Double> hourMap = new HashMap<>();
//        List<Map> mapList = orderMapper.selectOrderAmountHour(date);
//        for (Map map : mapList) {
//            hourMap.put((String) map.get("C_HOUR"), (Double) map.get("AMOUNT"));
//        }
//
//        return hourMap;
//    }

    @Override
    public Map getSaleDetail(String date, String keyword, int pageSize, int pageNo) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);
        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);
        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);
        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((pageNo - 1) * pageSize);
        searchSourceBuilder.size(pageSize);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_SALE_DETAIL).addType("_doc").build();
        //需要总数， 明细，2个聚合的结果
        Map resultMap = new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //总数
            Long total = searchResult.getTotal();

            //明细
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            List<Map> saleDetailList = new ArrayList<>();
            for (SearchResult.Hit<Map, Void> hit : hits) {
                saleDetailList.add(hit.source);
            }
            //年龄聚合结果
            Map ageMap = new HashMap();
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_user_age").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                ageMap.put(bucket.getKey(), bucket.getCount());
            }
            //性别聚合结果
            Map<String, Object> genderMap = new HashMap<>(4);
            List<TermsAggregation.Entry> genderbuckets = searchResult.getAggregations().getTermsAggregation("groupby_user_gender").getBuckets();
            for (TermsAggregation.Entry bucket : genderbuckets) {
                genderMap.put(bucket.getKey(), bucket.getCount());
            }

            resultMap.put("total", total);
            resultMap.put("list", saleDetailList);
            resultMap.put("ageMap", ageMap);
            resultMap.put("genderMap", genderMap);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }
}
