package pingle.wang.client.table;

import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * @Author: wpl
 */
public interface FlinkTableCatalog extends ExternalCatalog {
    String getType();

    /**
     * 获取Append类型的sink
     * @param table
     * @return
     * @throws IOException
     */
    AppendStreamTableSink<Row> getAppendStreamTableSink(ExternalCatalogTable table) throws IOException;

    /**
     * 获取Upsert类型的sink
     * @param table
     * @return
     * @throws IOException
     */
    UpsertStreamTableSink<Row> getUpsertStreamTableSink(ExternalCatalogTable table) throws IOException;

    /**
     * 获取Batch类型的sink
     * @param table
     * @return
     * @throws IOException
     */
    BatchTableSink<Row> getBatchTableSink(ExternalCatalogTable table) throws IOException;
}
