package util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;

/**
 * Created by zhuzhenghuan on 2017/10/16.
 */
public class MySQLDataSource {

    public static Dataset<Row> sources;

    private MySQLDataSource() {

    }

    public synchronized static Dataset<Row> getDataSource() {
        if (Objects.isNull(sources)) {
            sources = loadData();
        }
        return sources;
    }

    private static Dataset<Row> loadData() {
        Properties connProperties = new Properties();
        Resource jdbcResource = new ClassPathResource("jdbc.properties");
        Resource loadDataResource = new ClassPathResource("sql.txt");

        String sqlText = "";
        try (
                InputStream jdbcIn = jdbcResource.getInputStream();
                InputStream loadDataIn = loadDataResource.getInputStream();
        ) {
            connProperties.load(jdbcIn);
            BufferedReader reader = new BufferedReader(new InputStreamReader(loadDataIn));
            StringBuilder content = new StringBuilder();
            String line = null;
            while((line = reader.readLine())!=null){
                content.append(line);
            }

            sqlText = content.toString();
        } catch (Exception ex) {
            throw new RuntimeException("MySQLDataSource.loadData() execute Failed! cause is:",ex);
        }
        long start = System.currentTimeMillis();

        SparkSession spark = SparkSession
                .builder()
                .master("local[12]")
                .appName("simpleAppName")
                .getOrCreate();
        String jdbcUrl = connProperties.getProperty("jdbc_url");
        Dataset<Row> operationDetail = spark.read().jdbc(jdbcUrl,"operation_detail",connProperties);
        Dataset<Row> orderCheck = spark.read().jdbc(jdbcUrl,"order_check02",connProperties);
        operationDetail.createOrReplaceTempView("operation_detail");
        orderCheck.createOrReplaceTempView("order_check");
        Dataset<Row> result = spark.sql(sqlText);
//        result.count();
        result.cache();
        long end = System.currentTimeMillis();
        System.out.println("load data cost time is:"+(end - start)/1000);

        return result;
    }

}
