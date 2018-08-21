package pingle.wang.sqlserver.sql.plan;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.sources.TableSource;
import pingle.wang.client.common.sql.SqlConstant;
import pingle.wang.client.job.CompilationResult;
import pingle.wang.client.job.JobCompiler;
import pingle.wang.client.job.JobDescriptor;
import pingle.wang.client.table.FlinkTableCatalog;
import pingle.wang.client.table.FlinkTableSink;
import pingle.wang.sqlserver.sql.parser.impl.SqlParserImpl;

import java.io.StringReader;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * @Author: wpl
 */
public class Planner {
    private static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;

    private Map<String, FlinkTableCatalog> inputs;
    private Map<String, TableSource> tableSourceMap;
    private LinkedList<FlinkTableSink> mlinkTableSinks;
    private   Map<String, String>  extraProps;

    //含有视图解析的sql部分
    public CompilationResult sqlPlanner(Map<String,LinkedHashMap<String,String>> sqls, int parallelism) throws Throwable {
        Validator validator = new Validator();
        //视图验证
        if(sqls.containsKey(SqlConstant.VIEW)){
            //视图名，对应查询
            LinkedHashMap<String, String> viewMap = sqls.get(SqlConstant.VIEW);
            Collection<String> views =viewMap.values();
            for (String sql:views) {
                SqlNodeList stmts = parse(sql);
                validator.validateQuery(stmts);
            }
        }

        if (sqls.containsKey(SqlConstant.INSERT_INTO)) {
            LinkedHashMap<String, String> updateMap = sqls.get(SqlConstant.INSERT_INTO);
            Collection<String> values = updateMap.values();
            for (String sql : values) {
                SqlNodeList stmts = parse(sql);
                validator.validateDml(stmts);
            }
        }

        JobDescriptor job = new JobDescriptor(
                    validator.userDefinedFunctions(),
                    tableSourceMap,
                    mlinkTableSinks,
                    parallelism,
                    extraProps,
                    sqls
                );

        CompilationResult res = JobCompiler.compileJob(job);

        res.setAdditionalJars(validator.additionalResources());
        if (res.getRemoteThrowable() != null) {
            throw res.getRemoteThrowable();
        }
        return res;
    }

    @VisibleForTesting
    static SqlNodeList parse(String sql) throws Exception {
        // Keep the SQL syntax consistent with Flink
        try (StringReader in = new StringReader(sql)) {
            SqlParserImpl impl = new SqlParserImpl(in);

            // back tick as the quote
            impl.switchTo("BTID");
            impl.setTabSize(1);
            impl.setQuotedCasing(Lex.JAVA.quotedCasing);
            impl.setUnquotedCasing(Lex.JAVA.unquotedCasing);
            impl.setIdentifierMaxLength(DEFAULT_IDENTIFIER_MAX_LENGTH);
            return impl.SqlStmtsEof();
        }
    }
}
