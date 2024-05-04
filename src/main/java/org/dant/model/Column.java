package org.dant.model;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.jboss.logging.annotations.Once;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class Column {

    private String name;
    private String type;
    public Function<Group,Object> extractFromGroup;


    public Column(){
    }

    public Column(String name, String type) {
        this.name = name;
        this.type = type;
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

}
