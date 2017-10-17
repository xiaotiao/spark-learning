package test;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import service.ValeeraCheckSparkService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zhuzhenghuan on 2017/10/16.
 */
public class CheckClient {

    static {

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);


        rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%-6r [%p] %c - %m%n")));
    }

    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("consumer.xml");
        context.start();
        ValeeraCheckSparkService checkSparkService = (ValeeraCheckSparkService) context.getBean("valeeraCheckSparkService");

        String checkText = getCheckText();

        long start = System.currentTimeMillis();
        List<String> checkTexts = Lists.newArrayList();
        List<Boolean> reuslt = Lists.newArrayList();
        for(int i = 0;i<30;i++){
            checkTexts.add(checkText);
        }
        checkTexts.stream().parallel().forEach(x ->{
            reuslt.add(checkSparkService.listHitBySql(x));
        });
        long end = System.currentTimeMillis();

        System.out.println("cost time is:"+(end - start)/1000);
        System.out.println("result size = "+reuslt.size()+" back result: "+Arrays.toString(reuslt.toArray()));

    }

    private static String getCheckText(){
        String checkText = "";
        Resource checkTextResource = new ClassPathResource("check.txt");
        try {
            checkText = FileUtils.readFileToString(checkTextResource.getFile());
        } catch (IOException e) {
            throw new RuntimeException("CheckClient.getCheckText execute failed! cause is:",e);
        }
        return checkText;
    }
}
