package service.impl;

import util.MySQLDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import service.ValeeraCheckSparkService;

/**
 * Created by zhuzhenghuan on 2017/10/16.
 */
@Component
public class ValeeraCheckSparkServiceImpl implements ValeeraCheckSparkService,InitializingBean{

    @Override
    public boolean listHitBySql(String sqlText) {
        Dataset<Row> dataSource = MySQLDataSource.getDataSource();
        dataSource.createOrReplaceTempView("o");
        SparkSession spark = SparkSession
                .builder()
                .appName("simpleAppName")
                .getOrCreate();
        Dataset<Row> checkResult = spark.sql(sqlText);
        checkResult.show();
        return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        MySQLDataSource.getDataSource();
    }
}
