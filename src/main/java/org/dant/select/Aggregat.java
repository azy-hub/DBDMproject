package org.dant.select;

import org.dant.commons.TypeDB;

import java.util.ArrayList;
import java.util.List;

public class Aggregat {

    private String nameColumn;

    private String typeAggregat;

    private Condition HAVING;

    public Aggregat() {}

    public Aggregat(String nameColumn, String typeAggregat, Condition HAVING) {
        this.nameColumn = nameColumn;
        this.typeAggregat = typeAggregat;
        this.HAVING = HAVING;
    }

    public String getNameColumn() {
        return nameColumn;
    }

    public void setNameColumn(String nameColumn) {
        this.nameColumn = nameColumn;
    }

    public String getTypeAggregat() {
        return typeAggregat;
    }

    public void setTypeAggregat(String typeAggregat) {
        this.typeAggregat = typeAggregat;
    }

    public Condition getHAVING() {
        return HAVING;
    }

    public void setHAVING(Condition HAVING) {
        this.HAVING = HAVING;
    }

    public Object applyAggregat(List<List<Object>> listOfList, int index, String typeColumn) {
        if (typeAggregat.equals("SUM")) {
            switch (typeColumn) {
                case TypeDB.INT, TypeDB.SHORT,TypeDB.BYTE:
                    return listOfList.stream().mapToInt( list -> (int)list.get(index)).sum();
                case TypeDB.LONG:
                    return listOfList.stream().mapToLong( list -> (long)list.get(index)).sum();
                case TypeDB.DOUBLE:
                    return listOfList.stream().mapToDouble( list -> (double)list.get(index)).sum();
                case TypeDB.STRING:
                    return null;
            }
        }
        if (typeAggregat.equals("COUNT")) {
            if (!nameColumn.equals("*"))
                return listOfList.parallelStream().filter( list -> list.get(index) != null).toList().size();
            else
                return listOfList.size();
        }
        if (typeAggregat.equals("MAX")) {
            switch (typeColumn) {
                case TypeDB.INT, TypeDB.SHORT,TypeDB.BYTE:
                    return listOfList.stream().mapToInt( list -> (int)list.get(index)).max().getAsInt();
                case TypeDB.LONG:
                    return listOfList.stream().mapToLong( list -> (long)list.get(index)).max().getAsLong();
                case TypeDB.DOUBLE:
                    return listOfList.stream().mapToDouble( list -> (double)list.get(index)).max().getAsDouble();
                case TypeDB.STRING:
                    return null;
            }
        }
        if (typeAggregat.equals("MIN")) {
            switch (typeColumn) {
                case TypeDB.INT, TypeDB.SHORT,TypeDB.BYTE:
                    return listOfList.stream().mapToInt( list -> (int)list.get(index)).min().getAsInt();
                case TypeDB.LONG:
                    return listOfList.stream().mapToLong( list -> (long)list.get(index)).min().getAsLong();
                case TypeDB.DOUBLE:
                    return listOfList.stream().mapToDouble( list -> (double)list.get(index)).min().getAsDouble();
                case TypeDB.STRING:
                    return null;
            }
        }
        if (typeAggregat.equals("AVG")) {
            switch (typeColumn) {
                case TypeDB.INT, TypeDB.SHORT,TypeDB.BYTE:
                    return (double) listOfList.stream().mapToInt( list -> (int)list.get(index)).sum() / (double) listOfList.size();
                case TypeDB.LONG:
                    return (double) listOfList.stream().mapToLong( list -> (long)list.get(index)).sum() / (double) listOfList.size();
                case TypeDB.DOUBLE:
                    return listOfList.stream().mapToDouble( list -> (double)list.get(index)).sum() / (double) listOfList.size();
                case TypeDB.STRING:
                    return null;
            }
        }
        return null;
    }

}
