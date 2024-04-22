package org.dant.model;

import java.math.BigDecimal;
import java.util.List;

public class Condition {

    private String nameColumn;
    private String op;
    private Object value;

    public Condition() {
    }

    public Condition(String nameColumn, String op, Object value) {
        this.nameColumn = nameColumn;
        this.op = op;
        this.value = value;
    }

    public String getNameColumn() {
        return nameColumn;
    }

    public String getOp() {
        return op;
    }

    public Object getValue() {
        return value;
    }

    public void setNameColumn(String nameColumn) {
        this.nameColumn = nameColumn;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public boolean checkCondition(List<Object> list, int index, String type) {
        Object object = list.get(index);
        if (object == null)
            return false;
        switch (this.op) {
            case ">":
                if ( type.equals("BINARY") )
                    return ((String)this.value).compareTo((String) list.get(index)) < 0;
                if ( type.equals("INT64") )
                    return ((BigDecimal)this.value).byteValue() < (byte)list.get(index);
                if ( type.equals("DOUBLE") )
                    return ((BigDecimal)this.value).doubleValue() < (Double)list.get(index);
                break;
            case "=":
                if ( type.equals("BINARY") )
                    return (this.value).equals(list.get(index));
                if ( type.equals("INT64") )
                    return ((BigDecimal)this.value).byteValue() == (byte) list.get(index);
                if ( type.equals("DOUBLE") )
                    return ((BigDecimal)this.value).doubleValue() == (Double)list.get(index);
                break;
            case "<":
                if ( type.equals("BINARY") )
                    return ((String)this.value).compareTo((String) list.get(index)) > 0;
                if ( type.equals("INT64") )
                    return ((BigDecimal)this.value).byteValue() > (byte)list.get(index);
                if ( type.equals("DOUBLE") )
                    return ((BigDecimal)this.value).doubleValue() > (Double)list.get(index);
                break;
            default:
                break;
        }
        return false;
    }

}
