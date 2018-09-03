package pingle.wang.sqlserver.sql.plan;

import org.apache.calcite.sql.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.Path;
import pingle.wang.sqlserver.sql.parser.SqlCreateFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @Author: wpl
 */
public class Validator {
    private final PropertiesConfiguration conf = new PropertiesConfiguration();
    private final ArrayList<Path> additionalResources = new ArrayList<>();
    private final HashMap<String, String> userDefinedFunctions = new HashMap<>();

    private SqlInsert statement;
    private SqlSelect sqlQuery;

    public Configuration options() {
        return conf;
    }

    public ArrayList<Path> additionalResources() {
        return additionalResources;
    }

    public Map<String, String> userDefinedFunctions() {
        return userDefinedFunctions;
    }

    public void validateViewQuery(SqlNodeList query) {
        extract(query);
        validateExactlyOnceViewSelect(query);
    }

    public void validateDml(SqlNodeList query) {
        extract(query);
        validateExactlyOnceDml(query);
    }

    public void validateFunction(SqlNodeList query) {
        extract(query);
    }

    /**
     * Extract options and jars from the queries.
     */
    @VisibleForTesting
    void extract(SqlNodeList query) {
        for (SqlNode n : query) {
            if (n instanceof SqlSetOption) {
                extract((SqlSetOption) n);
            } else if (n instanceof SqlCreateFunction) {
                extract((SqlCreateFunction) n);
            }
        }
    }

    public void extract(SqlCreateFunction node) {
        if (node.jarList() == null) {
            return;
        }

        for (SqlNode n : node.jarList()) {
//      URI uri = URI.create(unwrapConstant(n));
            additionalResources.add(new Path(unwrapConstant(n)));
        }

        String funcName = node.dbName() != null ? unwrapConstant(node.dbName()) + "." + unwrapConstant(node.funcName())
                : unwrapConstant(node.funcName());
        String clazzName = unwrapConstant(node.className());
        userDefinedFunctions.put(funcName, clazzName);
    }

    public void extract(SqlSetOption node) {
        Object value = unwrapConstant(node.getValue());
        String property = node.getName().toString();

        Preconditions.checkArgument(!"SYSTEM".equals(node.getScope()),
                "cannot set properties at the system level");
        conf.setProperty(property, value);
    }

    @VisibleForTesting
    void validateExactlyOnceViewSelect(SqlNodeList query) {
        Preconditions.checkArgument(query.size() > 0);
        SqlNode last = query.get(query.size() - 1);
        long n = StreamSupport.stream(query.spliterator(), false)
                .filter(x -> x instanceof SqlSelect)
                .count();
        Preconditions.checkArgument(n == 1 && last instanceof SqlSelect,
                "Only one top-level SELECT statement is allowed");
        sqlQuery = (SqlSelect) last;
    }

    @VisibleForTesting
    void validateExactlyOnceDml(SqlNodeList query) {
        Preconditions.checkArgument(query.size() > 0);
        long n = StreamSupport.stream(query.spliterator(), false)
                .filter(x -> x instanceof SqlInsert)
                .count();
        System.out.println(n);
        Stream<SqlNode> nodeStream = StreamSupport.stream(query.spliterator(), false)
                .filter(x -> x instanceof SqlInsert);
        Iterator<SqlNode> iterator = nodeStream.iterator();
        for (Iterator<SqlNode> it = iterator; it.hasNext(); ) {
            SqlInsert node = (SqlInsert) it.next();
            statement = node;
        }
        Preconditions.checkArgument(n == 1 && statement instanceof SqlInsert,
                "Only one top-level Insert statement is allowed");

    }


    public SqlInsert statement() {
        return statement;
    }

    public SqlSelect sqlQuery() {
        return sqlQuery;
    }

    /**
     * Unwrap a constant in the AST as a Java Object.
     *
     * <p>The Calcite validator has folded all the constants by this point.
     * Thus the function expects either a SqlLiteral or a SqlIdentifier but not a SqlCall.</p>
     */
    private static String unwrapConstant(SqlNode value) {
        if (value == null) {
            return null;
        } else if (value instanceof SqlLiteral) {
            return ((SqlLiteral) value).toValue();
        } else if (value instanceof SqlIdentifier) {
            return value.toString();
        } else {
            throw new IllegalArgumentException("Invalid constant " + value);
        }
    }
}

