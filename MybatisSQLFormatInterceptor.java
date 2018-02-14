package com.mybatis.plugin;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.cxf.common.util.CollectionUtils;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * <p>mybatis sql 格式化,记录执行时间拦截器</p>
 * <p>使用方式:在mybatis 配置文件的sqlSessionFactory bean中设置plugins 属性.eg:
 * <property name="plugins">
 * <array>
 * <bean class="com.mybatis.plugin.MybatisSQLFormatInterceptor"/>
 * </array>
 * </property>
 * </p>
 * 或者 在 sqlSessionFactory 中引入 META-INF/spring/mybatis-config.xml <br>
 * property name="configLocation" value="classpath:META-INF/spring/mybatis-config.xml" />
 *
 * @author StefenKu
 * @version $Id: SQLFormatInterceptor.java, v 0.1 2017/7/24 18:14 StefenKu Exp $
 */
@Intercepts({@Signature(type = Executor.class, method = "query", args = {MappedStatement.class,
        Object.class,
        RowBounds.class,
        ResultHandler.class}),
        @Signature(type = Executor.class, method = "update", args = {MappedStatement.class,
                Object.class})

})
public class MybatisSQLFormatInterceptor implements Interceptor {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(MybatisSQLFormatInterceptor.class);

    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DB_TIMESTAMP_PATTERN = "yyyy-MM-dd HH24:MI:ss.ff";

    /**
     * 查询sql,查询结果达到警告级别的数量*
     */
    private int warnQueryResultNum = 1000;
    /**
     * 查询sql,查询结果达到警告级别的时间*
     */
    private int warnQueryResultCostTime = 5 * 1000;
    /**
     * 是否输出返回*
     */
    private boolean islogQueryResult;

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        Object result = null;

        long startTime = System.currentTimeMillis();
        try {
            result = invocation.proceed();
            return result;
        } finally {
            try {

                long endTime = System.currentTimeMillis();
                long costTime = endTime - startTime;
                MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];

                //取sql参数
                Object paramObject = invocation.getArgs()[1];

                BoundSql boundSql = mappedStatement.getBoundSql(paramObject);

                Configuration configuration = mappedStatement.getConfiguration();

                String sql = boundSql.getSql();
                Object parameterObject = boundSql.getParameterObject();
                List<ParameterMapping> parameterMappingList = boundSql.getParameterMappings();

                //格式化 sql
                sql = formatSql(sql, parameterObject, parameterMappingList, boundSql,
                        configuration);

                //如果是查询sql,统计查询结果数量
                Method method = invocation.getMethod();
                if ("query".equals(method.getName())) {
                    List queryResult = (List) result;
                    int num = queryResult != null ? queryResult.size() : 0;
                    if (num > getWarnQueryResultNum() || costTime > getWarnQueryResultCostTime()) {
                        LOGGER.warn("Execute sql:[{}],执行耗时:[{}]ms,查询记录数:[{}]", sql, costTime, num);
                    } else {
                        LOGGER.info("Execute sql:[{}],执行耗时:[{}]ms,查询记录数:[{}]", sql, costTime, num);
                    }

                    //打印返回结果
                    if (islogQueryResult) {
                        logQueryResult(queryResult);
                    }

                } else {
                    LOGGER.info("Execute sql:[{}],执行耗时:[{}]ms", sql, costTime);

                }

            } catch (Exception e) {
                LOGGER.error("格式化sql异常:", e);
            }

        }

    }

    private void logQueryResult(List queryResult) {
        if (!CollectionUtils.isEmpty(queryResult)) {
            if (queryResult.size() <= getWarnQueryResultNum()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < queryResult.size(); i++) {
                    builder.append(ToStringBuilder.reflectionToString(queryResult.get(i),
                            ToStringStyle.SHORT_PREFIX_STYLE) + "\n\t");
                }
                LOGGER.info("Execute sql result  : {}", builder);
            } else {
                LOGGER.warn("Execute sql result size gt warnQueryResultNum");
            }
        } else {
            LOGGER.info("Execute sql result is empty");
        }
    }

    private String formatSql(String sql, Object parameterObject,
                             List<ParameterMapping> parameterMappingList, BoundSql boundSql,
                             Configuration configuration) {

        // 输入sql字符串空判断
        if (sql == null || sql.length() == 0) {
            return "";
        }

        // 美化sql
        sql = beautifySql(sql);

        // 不传参数的场景，直接把Sql美化一下返回出去
        if (parameterObject == null || parameterMappingList == null
                || parameterMappingList.size() == 0) {
            return sql;
        }

        // 定义一个没有替换过占位符的sql，用于出异常时返回
        String sqlWithoutReplacePlaceholder = sql;

        try {
            sql = handleParameter(sql, parameterMappingList, boundSql, parameterObject,
                    configuration);
        } catch (Exception e) {
            LOGGER.error("sql format error:", e);
            // 占位符替换过程中出现异常，则返回没有替换过占位符但是格式美化过的sql，这样至少保证sql语句比BoundSql中的sql更好看
            return sqlWithoutReplacePlaceholder;
        }

        return sql;

    }

    /**
     * 参数处理
     */
    private String handleParameter(String sql, List<ParameterMapping> parameterMappingList,
                                   BoundSql boundSql, Object parameterObject,
                                   Configuration configuration) {
        TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();

        MetaObject metaObject = parameterObject == null ? null
                : configuration.newMetaObject(parameterObject);
        for (ParameterMapping parameterMapping : parameterMappingList) {
            Object propertyValue = null;
            if (parameterMapping.getMode() != ParameterMode.OUT) {
                String propertyName = parameterMapping.getProperty();
                if (boundSql.hasAdditionalParameter(propertyName)) {
                    propertyValue = boundSql.getAdditionalParameter(propertyName);
                } else if (parameterObject == null) {
                    propertyValue = null;
                } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
                    propertyValue = parameterObject;
                } else {
                    propertyValue = metaObject == null ? null : metaObject.getValue(propertyName);
                }
            }

            if (propertyValue == null) {
                propertyValue = "null";
            } else if (propertyValue.getClass().isAssignableFrom(String.class)) {
                propertyValue = String.format("'%s'", propertyValue);
            } else if (propertyValue.getClass().isAssignableFrom(Date.class)) {
                propertyValue = String.format("to_timestamp('%s', '%s')",
                        getNewFormatDateString((Date) propertyValue), DB_TIMESTAMP_PATTERN);
            }

            sql = sql.replaceFirst("\\?", propertyValue.toString());
        }

        return sql;
    }

    private String getNewFormatDateString(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);
        return getDateString(date, dateFormat);
    }

    private String getDateString(Date date, DateFormat dateFormat) {
        return date != null && dateFormat != null ? dateFormat.format(date) : null;
    }

    /**
     * 美化Sql
     */
    private String beautifySql(String sql) {
        sql = sql.replaceAll("[\\s\n ]+", " ");
        return sql;
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {

    }

    public int getWarnQueryResultNum() {
        return warnQueryResultNum;
    }

    public void setWarnQueryResultNum(int warnQueryResultNum) {
        this.warnQueryResultNum = warnQueryResultNum;
    }

    public int getWarnQueryResultCostTime() {
        return warnQueryResultCostTime;
    }

    public void setWarnQueryResultCostTime(int warnQueryResultCostTime) {
        this.warnQueryResultCostTime = warnQueryResultCostTime;
    }

    public void setIslogQueryResult(boolean islogQueryResult) {
        this.islogQueryResult = islogQueryResult;
    }
}
