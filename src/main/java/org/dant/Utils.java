package org.dant;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.dant.model.Column;
import org.dant.model.TypeDB;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    public static List<Object> extractListFromGroup(Group group, List<Column> columns) {
        List<Object> list = new ArrayList<>(columns.size());
        for (Column column : columns) {
           list.add(column.extractFromGroup.apply(group));
        }
        return list;
    }

    public static Object cast(Object object, String type) {
        return switch (type) {
            case TypeDB.DOUBLE -> ((BigDecimal) object).doubleValue();
            case TypeDB.STRING -> object.toString();
            case TypeDB.LONG -> ((BigDecimal) object).longValue();
            case TypeDB.INT -> ((BigDecimal) object).intValue();
            case TypeDB.SHORT -> ((BigDecimal) object).shortValue();
            case TypeDB.BYTE -> ((BigDecimal) object).byteValue();
            default -> null;
        };
    }

    public static List<Integer> intersectionSortedList(List<Integer> list1, List<Integer> list2) {
        List<Integer> list = new ArrayList<>();
        int j=0;
        int k=0;
        while(j<list1.size() && k<list2.size()) {
            if( list1.get(j).equals(list2.get(k)) ) {
                list.add(list1.get(j));
                j++;
                k++;
            }
            if( list1.get(j).compareTo(list2.get(k)) < 0 ) {
                j++;
            } else {
                k++;
            }
        }
        return list;
    }

}
