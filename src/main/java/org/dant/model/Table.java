package org.dant.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Table {

    private String name;
    private List<Column> columns;

    private List<List<Object>> rows;

    private final SpinLock lockAdd = new SpinLock();

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
        lockAdd.lock();
        try {
            rows.add(row);
        } finally {
            lockAdd.unlock();
        }
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
        return (conditions == null) || conditions.stream().allMatch(this::checkCondition);
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

    public List<Object> transform(List<Object> row) {
        List<Object> list = new ArrayList<>();
        for(int i=0; i<columns.size(); i++) {
            try {
                switch (columns.get(i).getType()) {
                    case "BINARY":
                        list.add( new String((byte[]) row.get(i), StandardCharsets.UTF_8));
                        break;
                    case "INT64":
                        list.add(row.get(i));
                        break;
                    case "DOUBLE":
                        list.add(ByteBuffer.wrap((byte[]) row.get(i)).getDouble());
                        break;
                    default:
                        break;
                }
            } catch (RuntimeException e) {
                list.add(null);
            }
        }
        return list;
    }

    public void addRowFromSimpleGroup(SimpleGroup simpleGroup) {
        List<Object> list = new ArrayList<>();
        for (Column column : getColumns()) {
            try {
                switch (column.getType()) {
                    case "DOUBLE":
                        list.add((ByteBuffer.allocate(Double.BYTES).putDouble(simpleGroup.getDouble(column.getName(), 0)).array().clone()));
                        break;
                    case "BINARY":
                        list.add(simpleGroup.getBinary(column.getName(), 0).getBytes());
                        break;
                    case "INT64":
                        list.add((byte) simpleGroup.getLong(column.getName(), 0));
                        break;
                    default:
                        list.add(null);
                        break;
                }
            } catch (RuntimeException e) {
                list.add(null);
            }
        }
        addRow(list);
    }
}
