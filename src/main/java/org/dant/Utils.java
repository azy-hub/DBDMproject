package org.dant;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.dant.model.Column;

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

}
