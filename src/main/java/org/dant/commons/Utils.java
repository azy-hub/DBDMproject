package org.dant.commons;

import gnu.trove.TIntArrayList;
import org.apache.parquet.example.data.Group;
import org.dant.model.Column;
import org.dant.commons.TypeDB;

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

    public static List<Object> castRow(List<Object> args, List<Column> columns) {
        List<Object> list = new ArrayList<>(columns.size());
        for(int i=0; i<columns.size(); i++) {
            if (args.get(i) == null)
                list.add(null);
            else {
                switch (columns.get(i).getType()) {
                    case TypeDB.DOUBLE:
                        list.add(((BigDecimal) args.get(i)).doubleValue());
                        break;
                    case TypeDB.STRING:
                        list.add(args.get(i));
                        break;
                    case TypeDB.LONG:
                        list.add(((BigDecimal) args.get(i)).longValue());
                        break;
                    case TypeDB.INT:
                        list.add(((BigDecimal) args.get(i)).intValue());
                        break;
                    case TypeDB.SHORT:
                        list.add(((BigDecimal) args.get(i)).shortValue());
                        break;
                    case TypeDB.BYTE:
                        list.add(((BigDecimal) args.get(i)).byteValue());
                        break;
                    default:
                        list.add(null);
                        break;
                }
            }
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

    public static int getIdxColumnByName(List<Column> columnList, String name) {
        for(Column column : columnList) {
            if (column.getName().equals(name)) {
                return columnList.indexOf(column);
            }
        }
        return -1;
    }

}
