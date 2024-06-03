package org.dant.select;

import org.dant.commons.Utils;
import org.dant.model.Column;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class SelectMethod {

    private List<ColumnSelected> SELECT;
    private String FROM;
    private List<Condition> WHERE;
    private List<String> GROUPBY;

    private List<Having> HAVING;

    public SelectMethod() {
    }

    public SelectMethod(List<ColumnSelected> SELECT, String FROM, List<Condition> WHERE, List<String> GROUPBY, List<Having> HAVING) {
        this.FROM = FROM;
        this.WHERE = WHERE;
        this.GROUPBY = GROUPBY;
        this.SELECT = SELECT;
        this.HAVING = HAVING;
    }

    public List<ColumnSelected>getSELECT() {
        return SELECT;
    }

    public String getFROM() {
        return FROM;
    }

    public List<Condition> getWHERE() {
        return WHERE;
    }

    public void setSELECT(List<ColumnSelected> SELECT) {
        this.SELECT = SELECT;
    }

    public void setFROM(String FROM) {
        this.FROM = FROM;
    }

    public void setWHERE(List<Condition> WHERE) {
        this.WHERE = WHERE;
    }

    public List<String> getGROUPBY() {
        return GROUPBY;
    }

    public void setGROUPBY(List<String> GROUPBY) {
        this.GROUPBY = GROUPBY;
    }

    public List<ColumnSelected> getAGGREGAT() { return this.SELECT.stream().filter( columnSelected -> columnSelected.getTypeAggregat() != null ).collect(Collectors.toList()); }

    public List<Having> getHAVING() {
        return HAVING;
    }

    public void setHAVING(List<Having> HAVING) {
        this.HAVING = HAVING;
    }

    public List<Object> applyAllAggregats(List<Column> columns, List<List<Object>> list ) {
        List<Object> tmp = new LinkedList<>();
        this.getAGGREGAT().forEach(columnSelected ->  {
            if( columnSelected.getTypeAggregat().equalsIgnoreCase("COUNT") && columnSelected.getNameColumn().equals("*")) {
                tmp.add(columnSelected.applyAggregat(list, -1, null));
            } else {
                int idxOfAggregat = Utils.getIdxColumnByName(columns, columnSelected.getNameColumn());
                tmp.add(columnSelected.applyAggregat(list, idxOfAggregat, columns.get(idxOfAggregat).getType()));
            }
        });
        return tmp;
    }
}
