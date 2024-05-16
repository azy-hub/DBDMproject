package org.dant.model;

import org.apache.parquet.example.data.Group;
import org.dant.commons.TypeDB;
import org.dant.index.Index;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;


public class Column {

    private String name;
    private String type;
    public Function<Group,Object> extractFromGroup;
    public Function<Object,Object> fromBytesToObject;
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
                this.fromBytesToObject = bytes -> bytes != null ? Double.parseDouble(new String((byte[]) bytes,StandardCharsets.UTF_8)) : null;
                break;
            case TypeDB.STRING:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.name) != 0)
                        return group.getString(this.name,0).getBytes(StandardCharsets.UTF_8);
                    return null;
                };
                this.fromBytesToObject = bytes -> bytes != null ? new String((byte[]) bytes,StandardCharsets.UTF_8) : null;
                break;
            case TypeDB.LONG:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.name) != 0)
                        return group.getLong(this.name,0);
                    return null;
                };
                this.fromBytesToObject = bytes -> bytes != null ? Long.parseLong(new String((byte[]) bytes,StandardCharsets.UTF_8)) : null;
                break;
            case TypeDB.INT:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.name) != 0)
                        return group.getInteger(this.name,0);
                    return null;
                };
                this.fromBytesToObject = bytes -> bytes != null ? Integer.parseInt(new String((byte[]) bytes,StandardCharsets.UTF_8)) : null;
                break;
            case TypeDB.BYTE:
                this.extractFromGroup = (group) -> {
                    if (group.getFieldRepetitionCount(this.name) != 0)
                        return (byte) group.getInteger(this.name, 0);
                    return null;
                };
                this.fromBytesToObject = bytes -> bytes != null ? Byte.parseByte(new String((byte[]) bytes,StandardCharsets.UTF_8)) : null;
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
