package org.dant.select;

import java.util.List;

public class SelectMethod {

    private List<String> SELECT;
    private String FROM;
    private List<Condition> WHERE;
    private String GROUPBY;
    private Aggregat AGGREGAT;

    public SelectMethod() {
    }

    public SelectMethod(List<String> SELECT, String FROM, List<Condition> WHERE, String GROUPBY, Aggregat AGGREGAT) {
        this.SELECT = SELECT;
        this.FROM = FROM;
        this.WHERE = WHERE;
        this.GROUPBY = GROUPBY;
        this.AGGREGAT = AGGREGAT;
    }

    public List<String> getSELECT() {
        return SELECT;
    }

    public String getFROM() {
        return FROM;
    }

    public List<Condition> getWHERE() {
        return WHERE;
    }

    public void setSELECT(List<String> SELECT) {
        this.SELECT = SELECT;
    }

    public void setFROM(String FROM) {
        this.FROM = FROM;
    }

    public void setWHERE(List<Condition> WHERE) {
        this.WHERE = WHERE;
    }

    public String getGROUPBY() {
        return GROUPBY;
    }

    public void setGROUPBY(String GROUPBY) {
        this.GROUPBY = GROUPBY;
    }

    public Aggregat getAGGREGAT() {
        return AGGREGAT;
    }

    public void setAGGREGAT(Aggregat AGGREGAT) {
        this.AGGREGAT = AGGREGAT;
    }
}
