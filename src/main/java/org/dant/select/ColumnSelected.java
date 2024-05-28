package org.dant.select;

import org.dant.commons.TypeDB;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class ColumnSelected {

    private String nameColumn;

    private String typeAggregat;

    public ColumnSelected() {}

    public ColumnSelected(String nameColumn, String typeAggregat, Condition HAVING) {
        this.nameColumn = nameColumn;
        this.typeAggregat = typeAggregat;
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

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ColumnSelected that = (ColumnSelected) object;
        return Objects.equals(nameColumn, that.nameColumn) && Objects.equals(typeAggregat, that.typeAggregat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nameColumn, typeAggregat);
    }

    public Object applyAggregat(List<List<Object>> listOfList, int index, String typeColumn) {
        if (typeAggregat.equals("SUM")) {
            switch (typeColumn) {
                case TypeDB.INT, TypeDB.SHORT,TypeDB.BYTE:
                    return listOfList.stream().mapToInt( list -> (int)list.get(index)).sum();
                case TypeDB.LONG:
                    return listOfList.stream().mapToLong( list -> (long)list.get(index)).sum();
                case TypeDB.DOUBLE:
                    return (float) listOfList.stream().mapToDouble( list -> ((Float)list.get(index)).doubleValue()).sum();
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
                    return listOfList.parallelStream().mapToInt( list -> (int)list.get(index)).max().getAsInt();
                case TypeDB.LONG:
                    return listOfList.parallelStream().mapToLong( list -> (long)list.get(index)).max().getAsLong();
                case TypeDB.DOUBLE:
                    return (float) listOfList.parallelStream().mapToDouble( list -> ((Float)list.get(index)).doubleValue()).max().getAsDouble();
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
                    return (float) listOfList.stream().mapToDouble( list -> ((Float)list.get(index)).doubleValue() ).min().getAsDouble();
                case TypeDB.STRING:
                    return null;
            }
        }
        if (typeAggregat.equals("AVG")) {
            switch (typeColumn) {
                case TypeDB.INT, TypeDB.SHORT,TypeDB.BYTE:
                    return (float) (listOfList.stream().mapToInt( list -> (int)list.get(index)).sum() / (double) listOfList.size());
                case TypeDB.LONG:
                    return (float) (listOfList.stream().mapToLong( list -> (long)list.get(index)).sum() / (double) listOfList.size());
                case TypeDB.DOUBLE:
                    return (float) (listOfList.stream().mapToDouble( list -> ((Float)list.get(index)).doubleValue() ).sum() / (double) listOfList.size());
                case TypeDB.STRING:
                    return null;
            }
        }
        return null;
    }

}
