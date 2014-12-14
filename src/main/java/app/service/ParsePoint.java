package app.service;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.regex.Pattern;

/**
 * Created by guildford on 14-12-14.
 */
public class ParsePoint implements Function<String, Vector> {

    private static final Pattern COMMA = Pattern.compile(",");

    @Override
    public Vector call(String line) {
        String[] tok = COMMA.split(line);
        double[] point = new double[tok.length];
        for (int i = 0; i < tok.length; ++i) {
            point[i] = Double.parseDouble(tok[i]);
        }
        return Vectors.dense(point);
    }

}
