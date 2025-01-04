package tn.enit.spark.entity;


public class GeographicInsight {
    private String location;
    private long count;

    public GeographicInsight(String location, long count) {
        this.location = location;
        this.count = count;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
