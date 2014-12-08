package app.controller;

import app.example.Greeting;
import app.spark.SparkContextHolder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by guildford on 14-12-8.
 */
@RestController
@RequestMapping("spark")
public class MyController {

    private static final String template = "Hello, %s!";
    private static final AtomicLong counter = new AtomicLong();

    @RequestMapping("/start")
    public static Greeting greeting() {

        SparkContextHolder holder = new SparkContextHolder();

        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        holder.addSparkContext(conf);

//        holder.getNumAs();
        String logFile = "data/kmeans.txt";
        JavaRDD<String> logData = holder.get("Simple Application").textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("6");
            }
        }).count();

        return new Greeting(counter.incrementAndGet(), String.format(template, numAs));
    }

}
