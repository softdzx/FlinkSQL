package pingle.wang.sqlserver.sql.plan;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.sources.TableSource;
import pingle.wang.client.common.sql.SqlConstant;
import pingle.wang.client.job.CompilationResult;
import pingle.wang.client.job.JobCompiler;
import pingle.wang.client.job.JobDescriptor;
import pingle.wang.client.table.FlinkTableSink;
import pingle.wang.sqlserver.sql.parser.impl.SqlParserImpl;

import java.io.StringReader;
import java.util.*;

/**
 * @Author: wpl
 */
public class Planner {
    private static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;

    private Map<String, TableSource> tableSourceMap;
    private List<FlinkTableSink> flinkTableSinks;
    private Map<String, String>  jobProps;

    public Planner( Map<String, TableSource> tableSourceMap, List<FlinkTableSink> flinkTableSinks, Map<String, String> jobProps) {
        this.tableSourceMap = tableSourceMap;
        this.flinkTableSinks = flinkTableSinks;
        this.jobProps = jobProps;
    }

    public CompilationResult sqlPlanner(Map<String,List<String>> funMap,Map<String,LinkedHashMap<String,String>> sqls, int parallelism) throws Throwable {
        Validator validator = new Validator();

        //方法
        if(funMap.containsKey(SqlConstant.FUNCTION)){
            List<String> list = funMap.get(SqlConstant.FUNCTION);
            for (String sql:list) {
                SqlNodeList stmts = parse(sql);
                validator.validateFunction(stmts);
            }
        }

        //视图
        if(sqls.containsKey(SqlConstant.VIEW)){
            //视图名，对应查询
            LinkedHashMap<String, String> viewMap = sqls.get(SqlConstant.VIEW);
            Collection<String> views =viewMap.values();
            for (String sql:views) {
                SqlNodeList stmts = parse(sql);
                validator.validateViewQuery(stmts);
            }
        }

        //dml
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
                    flinkTableSinks,
                    parallelism,
                    jobProps,
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
