package app.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by guildford on 14-12-8.
 */
public class SparkContextPool {

    private static class SparkContextPoolHolder {
        private static final SparkContextPool INSTANCE = new SparkContextPool();
    }

    public static final SparkContextPool getInstance() {
        return SparkContextPoolHolder.INSTANCE;
    }

    private Map<String, JavaSparkContext> scMap = null;

    private final int capacity = 100;
    private final AtomicLong counter = new AtomicLong();

    public SparkContextPool(Map<String, JavaSparkContext> scMap) {
        this.scMap = this.scMap;
    }

    public SparkContextPool() {
        this.scMap = new HashMap<String, JavaSparkContext>();
    }

    public String addSparkContext(SparkConf conf) {
        if (this.scMap.containsKey(conf.get("spark.app.name"))) {
            String newName = conf.get("spark.app.name") + " " + counter.incrementAndGet();
            conf.setAppName(newName);
            this.scMap.put(newName, new JavaSparkContext(conf));
        } else {
            this.scMap.put(conf.get("spark.app.name"), new JavaSparkContext(conf));
        }

        return conf.get("spark.app.name");
    }

    public JavaSparkContext get(String appName) {
        return this.scMap.get(appName);
    }
}
