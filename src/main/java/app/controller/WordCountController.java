package app.controller;

import app.model.WordCount;
import app.spark.SparkContextPool;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by guildford on 14-12-9.
 */
@RestController
@RequestMapping("spark")
public class WordCountController {

    private static final SparkContextPool scPool = SparkContextPool.getInstance();

    @RequestMapping("wordcount")
    public static WordCount count() {

        String logFile = "data/kmeans.txt";
        JavaRDD<String> logData = scPool.get("wordcount").textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("6");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("5");
            }
        }).count();

        return new WordCount(numAs, numBs);
    }
}
