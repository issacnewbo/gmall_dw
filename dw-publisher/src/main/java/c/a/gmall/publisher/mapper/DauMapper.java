package c.a.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    /**
     * 查询某日用户活跃总数
     *
     */
    public Long selectDauTotal(String date);

    //查询某日用户活跃数的分时值
    public List<Map>  selectDauTotalHours(String date);
}
