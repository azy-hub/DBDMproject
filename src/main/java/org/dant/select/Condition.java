package org.dant.select;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.dant.commons.TypeDB;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

public class Condition {

    @JsonProperty("nameColumn")
    private String nameColumn;
    @JsonProperty("op")
    private String op;
    @JsonProperty("value")
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
        if (type.equals(TypeDB.DOUBLE)) {
            object = (object instanceof Double) ? ((Double)object).floatValue() : ((Float)object);
        }
        switch (this.op) {
            case ">":
                if ( type.equals(TypeDB.STRING) )
                    return ((String)this.value).compareTo((String) object) < 0;
                if ( type.equals(TypeDB.DOUBLE) )
                    return ((BigDecimal)this.value).floatValue() < (float) object;
                if ( type.equals(TypeDB.LONG) )
                    return ((BigDecimal)this.value).longValue() < (long) object;
                if ( type.equals(TypeDB.INT) )
                    return ((BigDecimal)this.value).intValue() < (int) object;
                if ( type.equals(TypeDB.SHORT) )
                    return ((BigDecimal)this.value).shortValue() < (short) object;
                if ( type.equals(TypeDB.BYTE) )
                    return ((BigDecimal)this.value).byteValue() < (byte) object;
                break;
            case "=":
                if ( type.equals(TypeDB.STRING) )
                    return (this.value).equals(object);
                if ( type.equals(TypeDB.DOUBLE) )
                    return ((BigDecimal)this.value).floatValue() == (float) object;
                if ( type.equals(TypeDB.LONG) )
                    return ((BigDecimal)this.value).longValue() == (long) object;
                if ( type.equals(TypeDB.INT) )
                    return ((BigDecimal)this.value).intValue() == (int) object;
                if ( type.equals(TypeDB.SHORT) )
                    return ((BigDecimal)this.value).shortValue() == (short) object;
                if ( type.equals(TypeDB.BYTE) )
                    return ((BigDecimal)this.value).byteValue() == (byte) object;
                break;
            case "<":
                if ( type.equals(TypeDB.STRING) )
                    return ((String)this.value).compareTo((String) object) > 0;
                if ( type.equals(TypeDB.DOUBLE) )
                    return ((BigDecimal)this.value).floatValue() > (float) object;
                if ( type.equals(TypeDB.LONG) )
                    return ((BigDecimal)this.value).longValue() > (long) object;
                if ( type.equals(TypeDB.INT) )
                    return ((BigDecimal)this.value).intValue() > (int) object;
                if ( type.equals(TypeDB.SHORT) )
                    return ((BigDecimal)this.value).shortValue() > (short) object;
                if ( type.equals(TypeDB.BYTE) )
                    return ((BigDecimal)this.value).byteValue() > (byte) object;
                break;
            case "<=":
                if ( type.equals(TypeDB.STRING) )
                    return ((String)this.value).compareTo((String) object) >= 0;
                if ( type.equals(TypeDB.DOUBLE) )
                    return ((BigDecimal)this.value).floatValue() >= (float) object;
                if ( type.equals(TypeDB.LONG) )
                    return ((BigDecimal)this.value).longValue() >= (long) object;
                if ( type.equals(TypeDB.INT) )
                    return ((BigDecimal)this.value).intValue() >= (int) object;
                if ( type.equals(TypeDB.SHORT) )
                    return ((BigDecimal)this.value).shortValue() >= (short) object;
                if ( type.equals(TypeDB.BYTE) )
                    return ((BigDecimal)this.value).byteValue() >= (byte) object;
                break;
            case ">=":
                if ( type.equals(TypeDB.STRING) )
                    return ((String)this.value).compareTo((String) object) <= 0;
                if ( type.equals(TypeDB.DOUBLE) )
                    return ((BigDecimal)this.value).floatValue() <= (float) object;
                if ( type.equals(TypeDB.LONG) )
                    return ((BigDecimal)this.value).longValue() <= (long) object;
                if ( type.equals(TypeDB.INT) )
                    return ((BigDecimal)this.value).intValue() <= (int) object;
                if ( type.equals(TypeDB.SHORT) )
                    return ((BigDecimal)this.value).shortValue() <= (short) object;
                if ( type.equals(TypeDB.BYTE) )
                    return ((BigDecimal)this.value).byteValue() <= (byte) object;
                break;
            case "!=":
                if ( type.equals(TypeDB.STRING) )
                    return !(this.value).equals(object);
                if ( type.equals(TypeDB.DOUBLE) )
                    return ((BigDecimal)this.value).floatValue() != (float) object;
                if ( type.equals(TypeDB.LONG) )
                    return ((BigDecimal)this.value).longValue() != (long) object;
                if ( type.equals(TypeDB.INT) )
                    return ((BigDecimal)this.value).intValue() != (int) object;
                if ( type.equals(TypeDB.SHORT) )
                    return ((BigDecimal)this.value).shortValue() != (short) object;
                if ( type.equals(TypeDB.BYTE) )
                    return ((BigDecimal)this.value).byteValue() != (byte) object;
                break;
            default:
                break;
        }
        return false;
    }

}
