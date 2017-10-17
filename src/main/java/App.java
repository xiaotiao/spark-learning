import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by zhuzhenghuan on 2017/10/11.
 */
public class App {

    JavaSparkContext sc;
    String jdbcUrl;
    Properties connProperties = new Properties();
    SparkSession spark;

    private void initW3Jdbc(){
        jdbcUrl = "jdbc:mysql://localhost:3309/bach_valeera?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull";
        connProperties.put("user","beta");
        connProperties.put("password","kVkBhpSVa6!3");
    }

    private void initLocalJdbc(){
        jdbcUrl = "jdbc:mysql://localhost:3306/test?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull";
        connProperties.put("user","root");
        connProperties.put("password","qsd19001008");
    }

    private void initBeta07Jdbc(){
        jdbcUrl = "jdbc:mysql://10.0.64.23:3309/bach_valeera?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull";
        connProperties.put("user","beta");
        connProperties.put("password","kVkBhpSVa6!3");
    }

    private void initSparkConf(){
        String masterUrl = "local[12]";
//        String masterUrl = "spark://zhuzhenghuandeMacBook-Pro.local:7077";
        SparkConf conf = new SparkConf().setAppName("simpleAppName").setMaster(masterUrl);
        sc = new JavaSparkContext(conf);
    }

    private void initSparkSession(){
        spark = SparkSession
                .builder()
//                .master("spark://zhuzhenghuandeMacBook-Pro.local:7077")
                .appName("simpleAppName")
//                .config("spark.some.config.option", "some-value")
                .getOrCreate();
    }

    public void setUp(){
        initW3Jdbc();
        initSparkConf();
        initSparkSession();
    }

    public void testLoadpwDataFromBeta0702() throws IOException {
        long start = System.currentTimeMillis();
        Dataset<Row> operationDetail = spark.read().jdbc(jdbcUrl,"operation_detail",connProperties);
        Dataset<Row> orderCheck = spark.read().jdbc(jdbcUrl,"order_check02",connProperties);

        long end = System.currentTimeMillis();

        System.out.println("load data cost time is:"+(end - start)/1000);

        operationDetail.createOrReplaceTempView("operation_detail");
        orderCheck.createOrReplaceTempView("order_check");

        start = System.currentTimeMillis();
//        Dataset<Row> result = spark.sql("select * from orderCheck");
//        String sqlText = FileUtils.readFileToString(new File("/Users/zhuzhenghuan/software/sql.txt"));
        String sqlText = FileUtils.readFileToString(new File("/home/zhenghuan.zhu/spark/sql.txt"));

//        String sqlText = FileUtils.readFileToString(new File("/Users/zhuzhenghuan/software/databases/sql1.txt"));
        Dataset<Row> result = spark.sql(sqlText);

//        result = result.limit(500000);



        System.out.println("count="+result.count());
        end = System.currentTimeMillis();


        System.out.println("load data cost time is:"+(end - start)/1000);

        result.createOrReplaceTempView("o");
        start = System.currentTimeMillis();
//        String checkText = FileUtils.readFileToString(new File("/Users/zhuzhenghuan/software/check.txt"));
        List<String> checkList = Collections.synchronizedList(new ArrayList<String>(50));
        for (int i=0;i<10; i++){
            String checkText = FileUtils.readFileToString(new File("/home/zhenghuan.zhu/spark/check.txt"));
            checkList.add(checkText);
        }

        checkList.stream().parallel().forEach(text ->{
            SparkSession spark = SparkSession
                    .builder()
                    .appName("simpleAppName")
                    .getOrCreate();
            Dataset<Row> checkResult = spark.sql(text);
            checkResult.show();
        });
        end = System.currentTimeMillis();
        System.out.println("check data cost time is:"+(end - start)/1000);

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Hello World!");
        App app = new App();
        app.setUp();
        app.testLoadpwDataFromBeta0702();

        while(true){
            Thread.sleep(2000);
        }
    }

//    public static void main(String[] args) {
//        System.out.println("hello world!");
//    }
}
