package app.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by guildford on 14-12-8.
 */
public class SparkContextHolder {

    //    private SparkConf conf = new SparkConf().setAppName("Simple Application");
    private Map<String, JavaSparkContext> scMap = new HashMap<String, JavaSparkContext>();

    public SparkContextHolder(Map<String, JavaSparkContext> scMap) {
        this.scMap = this.scMap;
    }

    public SparkContextHolder() {
    }

    public void addSparkContext(SparkConf conf) {
        this.scMap.put(conf.get("spark.app.name"), new JavaSparkContext(conf));
    }

    public JavaSparkContext get(String appName) {
        return this.scMap.get(appName);
    }
}
