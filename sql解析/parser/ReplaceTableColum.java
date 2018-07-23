/**
  替换用户输入的表名，列名中存在随意书写的问题，将这些关键字段统一为大写或者小写
 主要使用是先将用户输入的sql传入到calcite parser进行解析，解析后对应是SqlNode,变为String后表名，列名自动添加``处理，我们只要拿到``字段处理就可以了。
 **/
public class ReplaceTableColumn {
    /**
     * 解析sql获取SqlNode
     * @param sql
     * @return
     */
    public SqlNodeList getSqlNodeList(String sql)  {
        if (StringUtils.isBlank(sql)){
            return null;
        }

        StringReader in = new StringReader(sql);
        SqlParserImpl impl = new SqlParserImpl(in);

        // back tick as the quote
        impl.switchTo("BTID");
        impl.setTabSize(1);
        impl.setQuotedCasing(Lex.JAVA.quotedCasing);
        impl.setUnquotedCasing(Lex.JAVA.unquotedCasing);
        impl.setIdentifierMaxLength(DEFAULT_IDENTIFIER_MAX_LENGTH);

        try {
            return impl.SqlStmtsEof();
        } catch (ParseException e) {
            logger.error("input sql parse error : ",e);
        }

        return null;
    }

    /**
     * 替换sql中的列，表名称
     * @param sql
     * @return 替换成功的sql
     */
    public String replaceInputTableColumn(String sql){
        String regEx = "`(.*?)`";
        Pattern pattern = Pattern.compile(regEx);
        Matcher matcher = pattern.matcher(sql);

        String replaceAll = sql;
        while(matcher.find()){
            String group = matcher.group();
            //替换为大写
            replaceAll = replaceAll.replace(group, group.toUpperCase());
        }

        return replaceAll;
    }

}
}