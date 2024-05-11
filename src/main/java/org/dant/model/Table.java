package org.dant.model;

import org.dant.select.Aggregat;
import org.dant.select.Condition;
import org.dant.select.SelectMethod;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

public class Table {

    private String name;
    private List<Column> columns;
    private List<Column> indexedColumns;

    private List<List<Object>> rows;

    private final SpinLock lockAdd = new SpinLock();

    public Table() {
    }

    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = new ArrayList<>();
        for(Column column : columns) {
            this.columns.add( new Column(column.getName(), column.getType()) );
        }
        indexedColumns = new ArrayList<>();
        rows = new ArrayList<>();
        DataBase.get().put(name,this);
    }

    public void addRow(List<Object> row) {
        int nbRow = rows.size();
        rows.add(row);
        for(Column column : indexedColumns) {
            Map<Object,List<Integer>> map = column.getIndex();
            Object obj = row.get(columns.indexOf(column));
            if (!map.containsKey(obj))
                map.put(obj,new ArrayList<>());
            map.get(obj).add(nbRow);
        }
    }

    public void addAllRows(List<List<Object>> rows) {
        lockAdd.lock();
        try {
            this.rows.addAll(rows);
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

    public List<Column> getIndexedColumns() {
        return indexedColumns;
    }

    public void setIndexedColumns(List<Column> indexedColumns) {
        this.indexedColumns = indexedColumns;
    }

    public List<Column> getColumnsByNames(List<String> list) {
        if (list.contains("*")) {
            return columns;
        }
        return list.stream().map( nameColumn -> {
            for( Column column : columns) {
                if( column.getName().equals(nameColumn))
                    return column;
            }
            return null;
        } ).collect(Collectors.toList());
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
                    case TypeDB.DOUBLE:
                        return condition.getValue() instanceof BigDecimal;
                    case TypeDB.STRING:
                        return condition.getValue() instanceof String;
                    case TypeDB.LONG:
                        if (condition.getValue() instanceof BigDecimal)
                            return ((BigDecimal)condition.getValue()).stripTrailingZeros().scale() <= 0;

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

    public List<Object> transform(List<Object> row, List<Column> columnList) {
        return columnList.stream().map( column -> row.get(columns.indexOf(column)) ).collect(Collectors.toList());
    }

    public List<List<Object>> select(SelectMethod selectMethod) {
        List<List<Object>> res;
        List<Column> columnList = getColumnsByNames(selectMethod.getSELECT());

        if (selectMethod.getWHERE() == null || selectMethod.getWHERE().isEmpty()) {
            res = getRows().parallelStream()
                    .map( row -> transform(row,columnList) )
                    .collect(Collectors.toList());;
        } else {
            List<Integer> idx = getIndexOfColumnsByConditions(selectMethod.getWHERE());
            List<String> type = idx.stream().map(indice -> getColumns().get(indice).getType()).toList();

            res = getRows().parallelStream().filter(list -> validate(list, selectMethod.getWHERE(), idx, type))
                    .map( row -> transform(row,columnList) )
                    .collect(Collectors.toList());
        }

        if (selectMethod.getGROUPBY() != null && selectMethod.getAGGREGAT() != null && !selectMethod.getAGGREGAT().isEmpty()) {
            int idxOfColumnGroupBy = 0;
            for(Column column : columnList) {
                if (column.getName().equals(selectMethod.getGROUPBY())) {
                    idxOfColumnGroupBy = columnList.indexOf(column);
                }
            }
            Set<Object> set = new HashSet<>();
            int finalIdxOfColumnGroupBy = idxOfColumnGroupBy;
            res.stream().forEach(list -> set.add(list.get(finalIdxOfColumnGroupBy)) );
            List<List<Object>> listeResultat = new ArrayList<>();
            for(Object object : set) {
                List<Object> tmp = new ArrayList<>();
                tmp.add(object);
                List<List<Object>> listeObject = res.parallelStream().filter( list -> list.get(finalIdxOfColumnGroupBy).equals(object)).collect(Collectors.toList());
                for(Aggregat aggregat : selectMethod.getAGGREGAT()) {
                    int idxOfAggregat = 0;
                    for(Column column : columnList) {
                        if (column.getName().equals(aggregat.getNameColumn())) {
                            idxOfAggregat = columnList.indexOf(column);
                        }
                    }
                    tmp.add(aggregat.applyAggregat(listeObject, idxOfAggregat, columnList.get(idxOfAggregat).getType()));
                }
                listeResultat.add(tmp);
            }
            res = listeResultat;
        }

        return res;
    }

    public List<Object> castRow(List<Object> args) {
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

    public void createIndexColumn(List<Integer> cardinalite, int tailleEchantillon) {
        for(int i=0; i<cardinalite.size(); i++) {
            if(cardinalite.get(i) < tailleEchantillon*0.1) {
                columns.get(i).setIsIndex(true);
                columns.get(i).setIndex(new HashMap<>());
                indexedColumns.add(columns.get(i));
            }
        }
    }

}
