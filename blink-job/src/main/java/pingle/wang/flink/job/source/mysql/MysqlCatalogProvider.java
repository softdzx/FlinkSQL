package pingle.wang.flink.job.source.mysql;

import org.apache.flink.table.api.TableSchema;
import pingle.wang.client.table.FlinkTableCatalog;
import pingle.wang.flink.job.provider.AbstractKafkaSourceProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: wpl
 */
public class MysqlCatalogProvider extends AbstractKafkaSourceProvider {

    private static final String TYPE = "mysql";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public FlinkTableCatalog getOutputCatalog(Map<String, String> props, TableSchema schema) {
        return new MysqlTableCatalog(props,  schema);
    }

}
