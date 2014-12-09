package app.model;

/**
 * Created by guildford on 14-12-9.
 */
public class WordCount {

    private long numAs;
    private long numBs;

    public WordCount(long numAs, long numBs) {
        this.numAs = numAs;
        this.numBs = numBs;
    }

    public WordCount() {
    }

    public long getNumAs() {
        return numAs;
    }

    public void setNumAs(long numAs) {
        this.numAs = numAs;
    }

    public long getNumBs() {
        return numBs;
    }

    public void setNumBs(long numBs) {
        this.numBs = numBs;
    }
}
