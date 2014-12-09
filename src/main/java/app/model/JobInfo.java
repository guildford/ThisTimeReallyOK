package app.model;

/**
 * Created by guildford on 14-12-9.
 */
public class JobInfo {

    private String appName;
    private String master;

    public JobInfo(String appName, String master) {
        this.appName = appName;
        this.master = master;
    }

    public JobInfo() {
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }
}
