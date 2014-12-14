package app.controller;

import app.model.WordCount;
import app.service.ParsePoint;
import app.spark.SparkContextPool;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by guildford on 14-12-9.
 */
@RestController
@RequestMapping("spark")
public class MLCtrlr {

    private static final SparkContextPool scPool = SparkContextPool.getInstance();

    @RequestMapping("wordcount")
    public static WordCount count() {

        String logFile = "data/kmeans";
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

    @RequestMapping("kmeans")
    public static KMeansModel kmeans(
            @RequestParam(value = "dataSource", defaultValue = "") String dataSource,
            @RequestParam(value = "centerNum", defaultValue = "") String centerNum,
            @RequestParam(value = "iterTimes", defaultValue = "") String iterTimes
    ) {
//        if (args.length < 3) {
//            System.err.println(
//                    "Usage: JavaKMeans <input_file> <k> <max_iterations> [<runs>]");
//            System.exit(1);
//        }
        String inputFile = dataSource.replaceAll("Z", "/");
        System.out.println(inputFile);
        int k = Integer.parseInt(centerNum);
        int iterations = Integer.parseInt(iterTimes);
        int runs = 1;

//        if (args.length >= 4) {
//            runs = Integer.parseInt(args[3]);
//        }
        JavaSparkContext sc = SparkContextPool.getInstance().get("interactive");
        JavaRDD<String> lines = sc.textFile(inputFile);

        JavaRDD<Vector> points = lines.map(new ParsePoint());

        KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs, KMeans.K_MEANS_PARALLEL());

        System.out.println("Cluster centers:");
        for (Vector center : model.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = model.computeCost(points.rdd());
        System.out.println("Cost: " + cost);

        return model;
    }
}
