package org.dant.model;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Table {

    private String name;
    private List<Column> columns;

    private List<List<Object>> rows;

    public Table() {
    }

    public Table(String name) {
        this.name = name;
        columns = new ArrayList<>();
        rows = new LinkedList<>();
        DataBase.get().put(name,this);
    }

    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns;
        rows = new LinkedList<>();
        DataBase.get().put(name,this);
    }

    public void addRow(List<Object> row) {
        rows.add(row);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public void setRows(List<List<Object>> rows) {
        this.rows = rows;
    }

    public boolean checkCondition(Condition condition) {
        for(Column column : columns) {
            if (condition.getNameColumn().equals(column.getName())) {
                switch (column.getType()) {
                    case "BINARY":
                        return condition.getValue() instanceof String;
                    case "INT64":
                        if (condition.getValue() instanceof BigDecimal)
                            return ((BigDecimal)condition.getValue()).stripTrailingZeros().scale() <= 0;
                    case "DOUBLE":
                        return condition.getValue() instanceof BigDecimal;
                    default:
                        break;
                }
            }
        }
        return false;
    }

    public boolean checkConditions(List<Condition> conditions) {
        return conditions.stream().allMatch(this::checkCondition);
    }

    public int getIndexOfColumnByCondition (Condition condition) {
        for (Column column : columns) {
            if (condition.getNameColumn().equals(column.getName())) {
                return columns.indexOf(column);
            }
        }
        return -1;
    }

    public List<Integer> getIndexOfColumnsByConditions(List<Condition> conditions) {
        return conditions.stream().map(this::getIndexOfColumnByCondition).toList();
    }

    public boolean validate(List<Object> list, List<Condition> conditions, List<Integer> idx, List<String> type) {
        for(int i=0; i<conditions.size(); i++) {
            if (!conditions.get(i).checkCondition(list, idx.get(i), type.get(i)))
                return false;
        }
        return true;
    }
}
