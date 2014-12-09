package app.controller;

import app.model.JobInfo;
import app.spark.SparkContextHolder;
import org.apache.spark.SparkConf;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by guildford on 14-12-8.
 */
@RestController
@RequestMapping("spark")
public class JobController {

    private static SparkContextHolder holder = new SparkContextHolder();

    @RequestMapping("/submitJob")
    public static JobInfo submit(
            @RequestParam(value = "name", defaultValue = "Untitled Application") String name,
            @RequestParam(value = "master", defaultValue = "local") String master
    ) {

        SparkConf conf = new SparkConf().setAppName(name).setMaster(master);
        holder.addSparkContext(conf);

//        String logFile = "data/kmeans.txt";
//        JavaRDD<String> logData = holder.get("Simple Application").textFile(logFile).cache();
//
//        long numAs = logData.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                return s.contains("6");
//            }
//        }).count();

        return new JobInfo(name, master);
    }

}
