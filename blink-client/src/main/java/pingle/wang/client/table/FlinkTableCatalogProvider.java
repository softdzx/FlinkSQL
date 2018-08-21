package pingle.wang.client.table;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.TableSource;

import java.util.HashMap;

/**
 * @Author: wpl
 */
public interface FlinkTableCatalogProvider {
    String getType();

    FlinkTableCatalog getInputCatalog(HashMap<String,String> props, TableSchema schema);

    /**
     * @param props
     * @param schema
     * @return
     */
    TableSource getInputTableSource(HashMap<String,String> props, TableSchema schema);


    FlinkTableCatalog getOutputCatalog(HashMap<String,String> props,TableSchema schema);
}
