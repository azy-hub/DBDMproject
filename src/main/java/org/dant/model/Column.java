package org.dant.model;

import org.apache.parquet.example.data.Group;
import org.dant.commons.TypeDB;
import org.dant.index.Index;

import java.math.BigDecimal;
import java.util.function.Function;


public class Column {

    private String name;
    private String type;
    public Function<Group,Object> extractFromGroup;
    public Function<Object,Object> parseJson;
    private boolean isIndex;
    private Index index;
    private int number;

    public Column(){
    }

    public Column(String name, String type, int number) {
        this.name = name;
        this.type = type;
        this.isIndex = false;
        this.number = number;
        switch (this.type) {
            case TypeDB.DOUBLE:
                this.extractFromGroup = group -> {
                    if (group.getFieldRepetitionCount(this.number) != 0)
                        return (float) group.getDouble(this.number,0);
                    return null;
                };
                this.parseJson = object -> ((BigDecimal) object).floatValue();
                break;
            case TypeDB.STRING:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.number) != 0)
                        return group.getString(this.number,0);
                    return null;
                };
                this.parseJson = object ->  object;
                break;
            case TypeDB.LONG:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.number) != 0)
                        return group.getLong(this.number,0);
                    return null;
                };
                this.parseJson = object -> ((BigDecimal) object).longValue();
                break;
            case TypeDB.INT:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.number) != 0)
                        return group.getInteger(this.number,0);
                    return null;
                };
                this.parseJson = object -> ((BigDecimal) object).intValue();
                break;
            case TypeDB.BYTE:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.number) != 0)
                        return (byte) group.getInteger(this.number, 0);
                    return null;
                };
                this.parseJson = object -> ((BigDecimal) object).byteValue();
                break;
            default:
                break;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isIndex() {
        return isIndex;
    }

    public void setIsIndex(boolean index) {
        isIndex = index;
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }
}
