package org.dant.model;

import java.util.List;

public class SelectMethod {

    private List<String> SELECT;
    private String FROM;
    private List<Condition> WHERE;

    public SelectMethod() {
    }

    public SelectMethod(List<String> SELECT, String FROM, List<Condition> WHERE) {
        this.SELECT = SELECT;
        this.FROM = FROM;
        this.WHERE = WHERE;
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
}
