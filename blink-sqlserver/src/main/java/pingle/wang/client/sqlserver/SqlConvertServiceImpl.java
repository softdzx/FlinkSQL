package pingle.wang.client.sqlserver;

import com.google.common.base.Joiner;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.insert.Insert;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pingle.wang.client.common.sql.SqlInputException;
import pingle.wang.client.common.sql.SqlParserResDescriptor;

import java.io.StringReader;
import java.util.*;

import static pingle.wang.client.common.sql.SqlConstant.*;


/**
 * @Author: wpl
 */
public class SqlConvertServiceImpl implements SqlConvertService {
    private static final Logger logger = LoggerFactory.getLogger(SqlConvertServiceImpl.class);

    private final String flag = "=";

    public SqlConvertServiceImpl() {

    }

    @Override
    public Map<String,String> getCreateSourceSqlInfo(String sqlContext) throws Exception {
        if (StringUtils.isBlank(sqlContext)){
            logger.warn("sql is null");
            throw new SqlInputException("sql not null   ");
        }

        Map<String, String> result = new LinkedHashMap<>();

        Map<String, List<String>> listMap = transfromSqlClassify(sqlContext);
        Map<String, String> ddlSqls = getDdlSqls(listMap);
        Set<String> tableNames = ddlSqls.keySet();

        //通过Sink来获取Source
//        TODO: 优化点
        Map<String, String> sinkSqlInfo = getCreateSinkSqlInfo(sqlContext);
        Set<String> sinkNames = sinkSqlInfo.keySet();

        if (CollectionUtils.isNotEmpty(sinkNames)){
            tableNames.removeAll(sinkNames);
        }

        for (String tableName : tableNames) {
            String ddlSql = ddlSqls.get(tableName);
            result.put(tableName, ddlSql);
        }

        return result;
    }

    @Override
    public Map<String,String> getCreateSinkSqlInfo(String sqlContext) throws Exception {
        if (StringUtils.isBlank(sqlContext)){
            logger.warn("sql is null");
            throw new SqlInputException("sql not null   ");
        }

        Map<String, String> result = new LinkedHashMap<>();
        Map<String, List<String>> listMap = transfromSqlClassify(sqlContext);

        List<String> sinkNames = new LinkedList<>();
        //通过insert into判断sink
        List<String> lists = listMap.get(INSERT_INTO);
        if (CollectionUtils.isNotEmpty(lists)){
            for (String str : lists){
                SqlParserResDescriptor insertInotParser = sqlInsertInotParser(str);
                sinkNames.add(insertInotParser.getTableName());
            }
        }

        Map<String, String> ddlSqls = getDdlSqls(listMap);
        for (String sinkName : sinkNames) {
            String ddlSql = ddlSqls.get(sinkName);
            result.put(sinkName,ddlSql);
        }

        return result;
    }

    @Override
    public SqlParserResDescriptor sqlDdlParser(String ddlSql) throws Exception {
        if (null == ddlSql){
            throw new SqlInputException("sql not null   ");
        }

        SqlParserResDescriptor sqlParserResDescriptor = new SqlParserResDescriptor();
        Map<String,String> parms = new LinkedHashMap<>();
        Map<String,TypeInformation<?>> schemas = new LinkedHashMap<>();

        Statement stmt = getSqlStatement(ddlSql);
        if (stmt instanceof CreateTable) {
            CreateTable table = (CreateTable) stmt;

            //字段名和类型解析
            List<ColumnDefinition> columnDefinitionList = table.getColumnDefinitions();
            if (CollectionUtils.isNotEmpty(columnDefinitionList)){
                for (ColumnDefinition colInfo : columnDefinitionList) {
                    String columnName = colInfo.getColumnName();
                    ColDataType colDataType = colInfo.getColDataType();
                    String dataType = colDataType.getDataType().trim().toLowerCase();
                    schemas.put(columnName.trim().replace("`",""),getColumnType(dataType));
                }
            }

            //索引解析
            List<Index> indexes = table.getIndexes();
            List<String> indexNames = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(indexes)){
                for (Index index : indexes) {
                    List<String> indexColumnsNames = index.getColumnsNames();
                    for (String indexColumnsName : indexColumnsNames) {
                        indexNames.add(0,indexColumnsName.trim().replace("`",""));
                    }
                }
            }
            parms.put(INDEXE,Joiner.on(",").join(indexNames));

            //with参数获取
            List<String> optionsStrings = (List<String>) table.getTableOptionsStrings();
            if (CollectionUtils.isNotEmpty(optionsStrings)){
                String options = optionsStrings.get(1);
                if (StringUtils.isNotBlank(options)){
                    String[] values = options.trim().substring(1, options.length() - 1).split(",");
                    for (String value : values) {
                        if (value.contains(flag)){
                            String[] keyValue = value.split(flag);
                            parms.put(keyValue[0],keyValue[1]);
                        }
                    }
                }
            }

            sqlParserResDescriptor.setTableName(table.getTable().getName());
            sqlParserResDescriptor.setSqlInfo(stmt.toString());
        }

        sqlParserResDescriptor.setSchemas(schemas);
        sqlParserResDescriptor.setParms(parms);

        return sqlParserResDescriptor;
    }

    @Override
    public SqlParserResDescriptor sqlViewParser(String viewSql) throws Exception {
        if (null == viewSql){
            throw new SqlInputException("sql not null   ");
        }
        SqlParserResDescriptor sqlParserResDescriptor = new SqlParserResDescriptor();
        Statement stmt = getSqlStatement(viewSql);
        if (stmt instanceof CreateView) {
            CreateView view = (CreateView) stmt;
            Table table = view.getView();
            sqlParserResDescriptor.setTableName(table.getName());
            sqlParserResDescriptor.setSqlInfo(view.getSelectBody().toString());
        }

        return sqlParserResDescriptor;
    }

    @Override
    public SqlParserResDescriptor sqlInsertInotParser(String insertSql) throws Exception {
        if (null == insertSql){
            throw new SqlInputException("sql not null   ");
        }

        SqlParserResDescriptor sqlParserResDescriptor = new SqlParserResDescriptor();
        Statement stmt = getSqlStatement(insertSql);
        if (stmt instanceof Insert) {
            Insert insert = (Insert) stmt;
            Table table = insert.getTable();
            sqlParserResDescriptor.setTableName(table.getName());
            sqlParserResDescriptor.setSqlInfo(insert.toString());
        }

        return sqlParserResDescriptor;
    }

    @Override
    public Map<String, List<String>> transfromSqlClassify(String sqlContext) throws Exception {
        if (StringUtils.isBlank(sqlContext)){
            logger.warn("sql is null");
            throw new SqlInputException("sql not null   ");
        }

        logger.warn("user input sql is : "+sqlContext);

        //sql拆解
        List<String> sqls = splitSemiColon(sqlContext);
        if (CollectionUtils.isEmpty(sqls)){
            logger.warn("sqls is null");
            throw new SqlInputException("sqls is null");
        }

        Map<String,List<String>> result = new HashMap<>();
        //sql顺序的一致
        List<String> funList = new LinkedList<>();
        List<String> tableList = new LinkedList<>();
        List<String> viewList = new LinkedList<>();
        List<String> insertList = new LinkedList<>();

        for (String sql: sqls) {
            if (StringUtils.isNotBlank(sql)){
                //替换多余的空格
                String newSql = sql.trim().replaceAll(" +", " ").replaceAll("\\s+", " ");

                if (newSql.toUpperCase().startsWith(CREATE)){
                    if (newSql.toUpperCase().contains(TABLE)){
                        tableList.add(newSql+SQL_END_FLAG);
                    }

                    if (newSql.toUpperCase().contains(FUNCTION)){
                        funList.add(newSql+SQL_END_FLAG);
                    }

                    if (newSql.toUpperCase().contains(VIEW)){
                        viewList.add(newSql+SQL_END_FLAG);
                    }
                }else {
                    insertList.add(newSql+SQL_END_FLAG);
                }

            }
        }

        if (CollectionUtils.isNotEmpty(funList)){
            result.put(FUNCTION,funList);
        }

        if (CollectionUtils.isNotEmpty(insertList)){
            result.put(INSERT_INTO,insertList);
        }

        if (CollectionUtils.isNotEmpty(viewList)){
            result.put(VIEW,viewList);
        }

        if (CollectionUtils.isNotEmpty(tableList)){
            result.put(TABLE,tableList);
        }

        return result;
    }

    private Map<String,String> getDdlSqls(Map<String, List<String>> listMap) throws Exception {
        Map<String, String> tableMap = new HashMap<>();
        List<String> tables = listMap.get(TABLE);
        if (CollectionUtils.isNotEmpty(tables)){
            for (String str : tables){
                SqlParserResDescriptor sqlDdlParser = sqlDdlParser(str);
                tableMap.put(sqlDdlParser.getTableName(),str);
            }
        }
        return tableMap;
    }

    private List<String> splitSemiColon(String sqlContext) {
        boolean inQuotes = false;
        boolean escape = false;

        List<String> ret = new ArrayList<>();

        char quoteChar = '"';
        int beginIndex = 0;
        for (int index = 0; index < sqlContext.length(); index++) {
            char c = sqlContext.charAt(index);
            switch (c) {
                case ';':
                    if (!inQuotes) {
                        ret.add(sqlContext.substring(beginIndex, index));
                        beginIndex = index + 1;
                    }
                    break;
                case '"':
                case '\'':
                    if (!escape) {
                        if (!inQuotes) {
                            quoteChar = c;
                            inQuotes = !inQuotes;
                        } else {
                            if (c == quoteChar) {
                                inQuotes = !inQuotes;
                            }
                        }
                    }
                    break;
                default:
                    break;
            }

            if (escape) {
                escape = false;
            } else if (c == '\\') {
                escape = true;
            }
        }

        if (beginIndex < sqlContext.length()) {
            ret.add(sqlContext.substring(beginIndex));
        }

        return ret;
    }



    private Statement getSqlStatement(String sql){
        CCJSqlParserManager parser = new CCJSqlParserManager();
        try {
            return parser.parse(new StringReader(sql));
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return null;
    }

    private TypeInformation<?> getColumnType(String type){
        TypeInformation<?> basicTypeInfo = null ;
        switch (type) {
            case "string":
                basicTypeInfo=BasicTypeInfo.STRING_TYPE_INFO;
                break;
            case "boolean":
                basicTypeInfo=BasicTypeInfo.BOOLEAN_TYPE_INFO;
                break;
            case "byte":
                basicTypeInfo=BasicTypeInfo.BYTE_TYPE_INFO;
                break;
            case "short":
                basicTypeInfo=BasicTypeInfo.SHORT_TYPE_INFO;
                break;
            case "integer":
                basicTypeInfo=BasicTypeInfo.INT_TYPE_INFO;
                break;
            case "long":
                basicTypeInfo=BasicTypeInfo.LONG_TYPE_INFO;
                break;
            case "float":
                basicTypeInfo=BasicTypeInfo.FLOAT_TYPE_INFO;
                break;
            case "double":
                basicTypeInfo=BasicTypeInfo.DOUBLE_TYPE_INFO;
                break;
            case "character":
                basicTypeInfo=BasicTypeInfo.CHAR_TYPE_INFO;
                break;
            case "void":
                basicTypeInfo=BasicTypeInfo.VOID_TYPE_INFO;
                break;
            case "biginteger":
                basicTypeInfo=BasicTypeInfo.BIG_INT_TYPE_INFO;
                break;
            case "bigdecimal":
                basicTypeInfo=BasicTypeInfo.BIG_DEC_TYPE_INFO;
                break;
            case "date":
                basicTypeInfo=SqlTimeTypeInfo.DATE;
                break;
            case "time":
                basicTypeInfo=SqlTimeTypeInfo.TIME;
                break;
            case "timestamp":
                basicTypeInfo=SqlTimeTypeInfo.TIMESTAMP;
                break;
            default:
                break;
        }
        return basicTypeInfo;
    }
}
