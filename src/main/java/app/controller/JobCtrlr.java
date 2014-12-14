package app.controller;

import app.model.JobInfo;
import app.spark.SparkContextPool;
import org.apache.spark.SparkConf;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by guildford on 14-12-8.
 */
@RestController
@RequestMapping("spark")
public class JobCtrlr {

    private static SparkContextPool scPool = SparkContextPool.getInstance();

    @RequestMapping("/submitJob")
    public static JobInfo submit(
            @RequestParam(value = "name", defaultValue = "Untitled Application") String name,
            @RequestParam(value = "master", defaultValue = "local") String master
    ) {

        SparkConf conf = new SparkConf().setAppName(name).setMaster(master);
        String instanceName = scPool.addSparkContext(conf);

        return new JobInfo(instanceName, master);
    }

    @RequestMapping("initial")
    public static void initial() {

        //tachyon
//        SparkConf conf_tachyon = new SparkConf()
//                .setAppName("tachyon")
//                .setMaster("local");
//        String instanceName = scPool.addSparkContext(conf_tachyon);

        //interactive
        SparkConf conf_inter = new SparkConf()
                .setAppName("interactive")
                .setMaster("local");
        scPool.addSparkContext(conf_inter);
    }

}
