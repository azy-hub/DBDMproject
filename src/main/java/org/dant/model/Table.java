package org.dant.model;

import gnu.trove.TIntArrayList;
import org.dant.commons.Utils;
import org.dant.commons.SpinLock;
import org.dant.commons.TypeDB;
import org.dant.index.HashMapIndex;
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
        this.columns = new ArrayList<>(columns.size());
        for(Column column : columns) {
            this.columns.add( new Column(column.getName(), column.getType()) );
        }
        indexedColumns = new ArrayList<>();
        rows = new ArrayList<>();
        DataBase.get().put(name,this);
    }

    public void addRow(List<Object> row) {
        int idxRow = rows.size();
        rows.add(row);
        for(Column column : indexedColumns) {
            column.getIndex().addIndex(row.get(columns.indexOf(column)),idxRow);
        }
    }

    public void addAllRows(List<List<Object>> rows) {
        if(this.rows.isEmpty()) {
            createIndexedColumns(rows);
        }

        lockAdd.lock();
        try {
            List<List<Object>> tmp = new ArrayList<>(this.rows.size()+rows.size());
            tmp.addAll(this.rows);
            this.rows = tmp;
            for(List<Object> row : rows) {
                addRow(row);
            }
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

    public Column getColumnByName(String nameColumn) {
        for( Column column : columns) {
            if( column.getName().equals(nameColumn))
                return column;
        }
        return null;
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
                    case TypeDB.LONG,TypeDB.INT:
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

    public boolean checkSelectMethod(SelectMethod selectMethod) {
        if ( !((selectMethod.getWHERE() == null) || selectMethod.getWHERE().stream().allMatch(this::checkCondition)))
            return false;
        if ( getColumnsByNames(selectMethod.getSELECT()).isEmpty() )
            return false;
        if ( selectMethod.getGROUPBY() != null && selectMethod.getGROUPBY().isEmpty() ) {
            if ( selectMethod.getSELECT().stream().noneMatch(columnName -> columnName.equals(selectMethod.getGROUPBY()) ) )
                return false;
            if ( selectMethod.getAGGREGAT().stream().anyMatch( aggregat -> aggregat.getNameColumn().equals(selectMethod.getGROUPBY()) ) )
                return false;
            if ( selectMethod.getAGGREGAT().stream().anyMatch( aggregat -> !selectMethod.getSELECT().contains(aggregat.getNameColumn()) ) )
                return false;
        }
        return true;
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
        List<Column> columnList = getColumnsByNames(selectMethod.getSELECT());
        List<List<Object>> res = getRows().parallelStream()
                .map( row -> transform(row,columnList) )
                .collect(Collectors.toList());;
        List<Condition> conditions = selectMethod.getWHERE();
        if ( !(conditions == null || conditions.isEmpty())) {
            List<Condition> conditionsOnIndexedColumn = conditions.stream().filter( condition -> isConditionOnIndexedColumn(condition,columnList)).collect(Collectors.toList());
            if( !conditionsOnIndexedColumn.isEmpty() ) {
                int idxColumn = Utils.getIndexOfColumnByCondition(conditionsOnIndexedColumn.get(0),columnList);
                TIntArrayList idxRows = new TIntArrayList();
                TIntArrayList listOfIndex = columnList.get( idxColumn ).getIndex().getIndexsFromValue(Utils.cast(conditionsOnIndexedColumn.get(0).getValue(),columnList.get(idxColumn).getType()));
                if(listOfIndex != null) {
                    idxRows = listOfIndex;
                }
                for(int i=1; i<conditionsOnIndexedColumn.size(); i++) {
                    int indexOfColumn = Utils.getIndexOfColumnByCondition(conditionsOnIndexedColumn.get(i), columnList);
                    listOfIndex = columnList.get(indexOfColumn).getIndex().getIndexsFromValue(conditionsOnIndexedColumn.get(i).getValue());
                    if(listOfIndex != null) {
                        idxRows = Utils.intersectionSortedList(idxRows,listOfIndex);
                    } else {
                        idxRows = new TIntArrayList();
                        break;
                    }
                }
                List<List<Object>> resultat = new ArrayList<>(idxRows.size());
                for(int idx=0; idx<idxRows.size(); idx++) {
                    resultat.add( res.get(idxRows.get(idx)) );
                }
                res = resultat;
                conditions.removeAll(conditionsOnIndexedColumn);
            }
            if( !conditions.isEmpty()) {
                List<Integer> idx = Utils.getIndexOfColumnsByConditions(conditions, columnList);
                List<String> type = idx.stream().map(indice -> columnList.get(indice).getType()).toList();
                res = res.parallelStream().filter(list -> validate(list, conditions, idx, type))
                        .collect(Collectors.toList());
            }
        }

        if (selectMethod.getGROUPBY() != null && selectMethod.getAGGREGAT() != null && !selectMethod.getAGGREGAT().isEmpty()) {
            int idxOfColumnGroupBy = Utils.getIdxColumnByName(columnList, selectMethod.getGROUPBY());

            Map<Object, List<List<Object>>> groupBy = new HashMap<>();
            res.forEach(list -> {
                Object object = list.get(idxOfColumnGroupBy);
                groupBy.computeIfAbsent(object, k -> new ArrayList<>()).add(list);
            });

            List<List<Object>> resultat = new ArrayList<>(groupBy.keySet().size());
            groupBy.forEach( (obj,list) -> {
                List<Object> tmp = new ArrayList<>(1+selectMethod.getAGGREGAT().size());
                tmp.add(obj);
                for(Aggregat aggregat : selectMethod.getAGGREGAT()) {
                    int idxOfAggregat = Utils.getIdxColumnByName(columnList, aggregat.getNameColumn());
                    tmp.add(aggregat.applyAggregat(list, idxOfAggregat, columnList.get(idxOfAggregat).getType()));
                }
                resultat.add(tmp);
            });
            res = resultat;
        }

        return res;
    }

    public void createIndexedColumns(List<List<Object>> rows) {
        int tailleEchantillon = 20000;
        if (rows.size() > tailleEchantillon) {
            List<Set<Object>> echantillon = new ArrayList<>(this.columns.size());
            for (Column column : this.columns) {
                echantillon.add(new HashSet<>());
            }
            for (List<Object> list : rows.subList(0,tailleEchantillon)) {
                for (int j = 0; j < columns.size(); j++) {
                    Object obj = list.get(j);
                    if (obj != null)
                        echantillon.get(j).add(list.get(j));
                }
            }
            List<Integer> cardinalite = echantillon.stream().map(Set::size).collect(Collectors.toList());
            for(int i=0; i<cardinalite.size(); i++) {
                if(cardinalite.get(i) < tailleEchantillon*0.1) {
                    columns.get(i).setIsIndex(true);
                    columns.get(i).setIndex(new HashMapIndex());
                    indexedColumns.add(columns.get(i));
                }
            }
        }
    }

    public boolean isConditionOnIndexedColumn(Condition condition, List<Column> columnList) {
        return columnList.stream().anyMatch( column -> column.isIndex() && column.getName().equals(condition.getNameColumn()) && condition.getOp().equals("=") );
    }

}
