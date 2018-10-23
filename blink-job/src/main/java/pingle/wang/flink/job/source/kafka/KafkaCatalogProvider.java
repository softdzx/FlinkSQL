package pingle.wang.flink.job.source.kafka;

import org.apache.flink.table.api.TableSchema;
import pingle.wang.client.table.FlinkTableCatalog;
import pingle.wang.flink.job.provider.AbstractKafkaSourceProvider;

import java.util.Map;

/**
 * @Author: wpl
 */
public class KafkaCatalogProvider extends AbstractKafkaSourceProvider {

    private static final String TYPE = "kafka";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public FlinkTableCatalog getOutputCatalog(Map<String, String> props, TableSchema schema) {
        return new KafkaTableCatalog(props,schema);
    }


}
