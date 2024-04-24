package org.dant;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.dant.model.Column;

import java.util.ArrayList;
import java.util.List;

public class Utils {

    public static List<Object> extractListFromGroup(SimpleGroup simpleGroup, List<Column> columns) {
        List<Object> list = new ArrayList<>(columns.size());
        for (Column column : columns) {
            try {
                switch (column.getType()) {
                    case "DOUBLE":
                        list.add(simpleGroup.getDouble(column.getName(),0));
                        break;
                    case "BINARY":
                        list.add(simpleGroup.getString(column.getName(), 0));
                        break;
                    case "INT64":
                        list.add(simpleGroup.getLong(column.getName(), 0));
                        break;
                    default:
                        list.add(null);
                        break;
                }
            } catch (RuntimeException e) {
                list.add(null);
            }
        }
        return list;
    }

}
