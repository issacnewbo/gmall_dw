package c.a.gmall.canal.app;

import c.a.gmall.canal.util.KafkaSender;
import c.a.gmall.constant.GmallConstants;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType eventType;
    String tableName;
    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle() {
        if ("order_info".equals(tableName) && eventType == CanalEntry.EventType.INSERT) {
            sendRowDataListToTopic(GmallConstants.KAFKA_TOPIC_NEW_ORDER);
        } else if ("user_info".equals(tableName) && eventType == CanalEntry.EventType.INSERT) {
            sendRowDataListToTopic(GmallConstants.KAFKA_TOPIC_NEW_USER);
        } else if ("order_detail".equals(tableName) && eventType == CanalEntry.EventType.INSERT) {
            sendRowDataListToTopic(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        }
    }

    public void sendRowDataListToTopic(String topic) {
        for (CanalEntry.RowData rowData : rowDataList) {

            JSONObject jsonObject = new JSONObject();

            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                String name = column.getName();
                String value = column.getValue();
                jsonObject.put(name, value);
            }

            String json = JSON.toJSONString(jsonObject);

            KafkaSender.send(topic, json);
        }
    }

}
