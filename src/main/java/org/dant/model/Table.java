package org.dant.model;

import java.util.ArrayList;
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
        rows = new ArrayList<>();
        DataBase.get().put(name,this);
    }

    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns;
        rows = new ArrayList<>();
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
}
