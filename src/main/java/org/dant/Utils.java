package org.dant;

import gnu.trove.TIntArrayList;
import gnu.trove.TIntIterator;
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

    public static TIntArrayList intersectionSortedList(TIntArrayList list1, TIntArrayList list2) {
        TIntArrayList list = new TIntArrayList();
        int j=0;
        int k=0;
        while(j<list1.size() && k<list2.size()) {
            if( list1.getQuick(j) == list2.getQuick(k) ) {
                list.add(list1.get(j));
                j++;
                k++;
            }
            if( list1.getQuick(j) < (list2.getQuick(k)) ) {
                j++;
            } else {
                k++;
            }
        }
        return list;
    }

}
