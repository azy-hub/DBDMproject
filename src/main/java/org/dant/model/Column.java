package org.dant.model;

import org.apache.parquet.example.data.Group;
import org.dant.commons.TypeDB;
import org.dant.index.Index;

import java.util.function.Function;


public class Column {

    private String name;
    private String type;
    public Function<Group,Object> extractFromGroup;
    private boolean isIndex;
    private Index index;



    public Column(){
    }

    public Column(String name, String type) {
        this.name = name;
        this.type = type;
        this.isIndex = false;
        switch (this.type) {
            case TypeDB.DOUBLE:
                this.extractFromGroup = group -> {
                    if (group.getFieldRepetitionCount(this.name) != 0)
                        return group.getDouble(this.name,0);
                    return null;
                };
                break;
            case TypeDB.STRING:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.name) != 0)
                        return group.getString(this.name,0);
                    return null;
                };
                break;
            case TypeDB.LONG:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.name) != 0)
                        return group.getLong(this.name,0);
                    return null;
                };
                break;
            case TypeDB.INT:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.name) != 0)
                        return group.getInteger(this.name,0);
                    return null;
                };
                break;
            case TypeDB.BYTE:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.name) != 0)
                        return (byte) group.getInteger(this.name, 0);
                    return null;
                };
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
}
