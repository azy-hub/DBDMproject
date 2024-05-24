package org.dant.select;

import org.dant.commons.Utils;
import org.dant.model.Column;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SelectMethod {

    private List<String> SELECT;
    private String FROM;
    private List<Condition> WHERE;
    private String GROUPBY;
    private List<Aggregat> AGGREGAT;

    public SelectMethod() {
    }

    public SelectMethod(List<String> SELECT, String FROM, List<Condition> WHERE, String GROUPBY, List<Aggregat> AGGREGAT) {
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

    public List<Aggregat> getAGGREGAT() {
        return AGGREGAT;
    }

    public void setAGGREGAT(List<Aggregat> AGGREGAT) {
        this.AGGREGAT = AGGREGAT;
    }

    public List<Object> applyAllAggregats(List<Column> columns, List<List<Object>> list ) {
        List<Object> tmp = new LinkedList<>();
        this.AGGREGAT.parallelStream().forEach( aggregat ->  {
            if( aggregat.getTypeAggregat().equals("COUNT") && aggregat.getNameColumn().equals("*")) {
                tmp.add(aggregat.applyAggregat(list, -1, null));
            } else {
                int idxOfAggregat = Utils.getIdxColumnByName(columns, aggregat.getNameColumn());
                tmp.add(aggregat.applyAggregat(list, idxOfAggregat, columns.get(idxOfAggregat).getType()));
            }
        });
        return tmp;
    }
}
