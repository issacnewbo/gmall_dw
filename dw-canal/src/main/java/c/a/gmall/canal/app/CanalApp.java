package c.a.gmall.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {
    public static void main(String[] args) {
        // 1 连接canal的服务器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");

        // 2 抓取数据
        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall_realtime.*");
            Message message = canalConnector.get(10);

            if (message.getEntries().size() == 0) {
                try {
                    // 没有数据休息5秒
                    System.out.println("休息 5 秒钟");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                // 3 抓取数据后，提取数据
                for (CanalEntry.Entry entry : message.getEntries()) {

                    if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        if (rowChange != null) {
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            // 4 处理业务数据  发送到kafka对应的topic中
                            CanalHandler canalHandler = new CanalHandler(rowChange.getEventType(), entry.getHeader().getTableName(), rowDatasList);
                            canalHandler.handle();
                        }
                    }
                }
            }
        }

    }
}
