package service;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by zhuzhenghuan on 2017/10/16.
 */
public interface ValeeraCheckSparkService {

   boolean listHitBySql(String sqlText);
}
