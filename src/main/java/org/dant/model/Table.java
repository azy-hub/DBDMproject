package org.dant.model;

import gnu.trove.TIntArrayList;
import org.dant.commons.Utils;
import org.dant.commons.SpinLock;
import org.dant.commons.TypeDB;
import org.dant.index.IndexFactory;
import org.dant.select.ColumnSelected;
import org.dant.select.Condition;
import org.dant.select.Having;
import org.dant.select.SelectMethod;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Table {

    private String name;
    private List<Column> columns;
    private List<Column> indexedColumns;
    private ArrayList<List<Object>> rows;
    private boolean isIndexed;
    private final Lock lockAdd = new ReentrantLock();

    public Table() {
    }

    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = new ArrayList<>(columns.size());
        int i=0;
        for(Column column : columns) {
            this.columns.add( new Column(column.getName(), column.getType(),i++) );
        }
        indexedColumns = new ArrayList<>();
        rows = new ArrayList<>();
        DataBase.get().put(name,this);
        this.isIndexed = false;
    }

    public void addRow(List<Object> row) {
        int idx = rows.size();
        rows.add(row);
        for(Column column : indexedColumns) {
            Object object = row.get(column.getNumber());
            if (object !=null)
                column.getIndex().addIndex(object, idx);
        }
    }

    public void addAllRows(List<List<Object>> lignes) {
        lockAdd.lock();
        final int[] rowsIdx = {rows.size()};
        Thread thread = new Thread(() -> {
            int index = rowsIdx[0];
            for(List<Object> row : lignes) {
                for (Column indexedColumn : indexedColumns) {
                    Object object = row.get(indexedColumn.getNumber());
                    if (object != null)
                        indexedColumn.getIndex().addIndex(object, index);
                }
                index++;
            }
        });
        thread.start();
        rows.addAll(lignes);
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        lockAdd.unlock();
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

    public boolean getIsIndexed() { return isIndexed; };

    public Column getColumnByName(String nameColumn) {
        for( Column column : columns) {
            if( column.getName().equals(nameColumn))
                return column;
        }
        return null;
    }

    public List<Column> getColumnsByNames(List<ColumnSelected> list) {
        if (list.stream().anyMatch( columnSelected -> columnSelected.getNameColumn().equalsIgnoreCase("*") ) ) {
            return columns;
        }
        return list.stream().map( columnSelected -> {
            for( Column column : columns) {
                if( column.getName().equalsIgnoreCase(columnSelected.getNameColumn()))
                    return column;
            }
            return null;
        } ).collect(Collectors.toList());
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public ArrayList<List<Object>> getRows() {
        return rows;
    }

    public void setRows(ArrayList<List<Object>> rows) {
        this.rows = rows;
    }

    public boolean checkCondition(Condition condition) {
        for(Column column : columns) {
            if (condition.getNameColumn().equalsIgnoreCase(column.getName())) {
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

    public boolean checkColumnExist(List<Column> columnList, String nameColumn) {
        for(Column column : columnList) {
            if (column.getName().equalsIgnoreCase(nameColumn))
                return true;
        }
        return false;
    }

    public boolean checkSelectMethod(SelectMethod selectMethod) {
        if ( selectMethod.getWHERE() != null && !selectMethod.getWHERE().isEmpty() && !selectMethod.getWHERE().stream().allMatch(this::checkCondition) ) {
            System.out.println("La valeure saisie dans le WHERE ne correspond pas au type de la colonne");
            return false;
        }
        for (ColumnSelected columnSelected : selectMethod.getSELECT()) {
            if( !columnSelected.getNameColumn().equalsIgnoreCase("*")) {
                if (!checkColumnExist(columns, columnSelected.getNameColumn())) {
                    System.out.println("La colonne " + columnSelected.getNameColumn() + " n'existe pas.");
                    return false;
                }
            }
        }
        List<Column> columnList = getColumnsByNames(selectMethod.getSELECT());
        if (selectMethod.getGROUPBY()!=null && !selectMethod.getGROUPBY().isEmpty()) {
            for (String nameColumn : selectMethod.getGROUPBY()) {
                if (!checkColumnExist(columns, nameColumn)) {
                    System.out.println("La colonne du groupby " + nameColumn + " n'existe pas dans les colonnes selectionné.");
                    return false;
                }
            }
        }
        if (selectMethod.getHAVING()!=null && !selectMethod.getHAVING().isEmpty()) {
            for(Having having : selectMethod.getHAVING()) {
                if(!checkColumnExist(columnList, having.getAggregate().getNameColumn())) {
                    System.out.println("La colonne du having " + having.getAggregate().getNameColumn() + " n'existe pas dans les colonnes selectionné.");
                    return false;
                }
                if(!having.checkCondition()) {
                    System.out.println("La valeur de la condition du having ne correspond pas au type.");
                    return false;
                }
            }
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
        // Récupérer la liste des colonnes qu'on séléctionne
        List<Column> columnList = getColumnsByNames(selectMethod.getSELECT());
        List<List<Object>> res = rows;
        // Récupère les conditions qui ont été soumise dans le WHERE
        List<Condition> conditions = new ArrayList<>(selectMethod.getWHERE());
        if (!conditions.isEmpty()) {
            // Filtre par les conditions qui sont sur des colonnes indéxé
            res = filterRowsWithIndexedColumn(res, conditions, columnList);
            // Filtre par les conditions qui sont pas
            res = filterRowsWithoutIndexedColumn(res, conditions);
        }
        List<ColumnSelected> aggregats = selectMethod.getAGGREGAT();
        // Vérifie si un groupBy et un aggrégat ont été demandé dans la requete SELECT
        if ( aggregats != null && !aggregats.isEmpty()) {
            if ( selectMethod.getGROUPBY() != null && !selectMethod.getGROUPBY().isEmpty()) {
                //int idxOfColumnGroupBy = Utils.getIdxColumnByName(columns, selectMethod.getGROUPBY()); // Trouve l'index de la colonne à regrouper parmis les colonnes selectionnées
                List<Integer> idxOfColumnsGroupBy = selectMethod.getGROUPBY().stream().map( nameColumn -> Utils.getIdxColumnByName(columns, nameColumn)).toList();
                Map<List<Object>, List<List<Object>>> groupBy = new HashMap<>();
                // Parcours chaque ligne et regroupe chaque valeur avec les lignes qui lui correspondent
                SpinLock groupByLock = new SpinLock();
                res.parallelStream().forEach(list -> {
                    List<Object> object = idxOfColumnsGroupBy.stream().map(list::get).toList();
                    groupByLock.lock();
                    groupBy.computeIfAbsent(object, k -> new ArrayList<>()).add(list);
                    groupByLock.unlock();
                });
                // Parcours la map générer grace au regroupement et applique les aggrégats demandés
                List<List<Object>> resultat = new ArrayList<>();
                groupBy.keySet().stream().forEach( obj -> {
                    List<Object> tmp = selectMethod.applyAllAggregats(columns, groupBy.get(obj));
                    tmp.add(0, obj);
                    resultat.add( tmp );
                });
                res = resultat;
            } else {
                List<List<Object>> resultat = new ArrayList<>();
                resultat.add( selectMethod.applyAllAggregats(columns, res) );
                res = resultat;
            }
        } else {
            // Filtrer les colonnes que l'on veut afficher
            res = res.parallelStream().map( row -> transform(row,columnList) ).collect(Collectors.toList());
        }

        return res;
    }


    private List<List<Object>> filterRowsWithoutIndexedColumn(List<List<Object>> res, List<Condition> conditions) {
        if ( !conditions.isEmpty() ) {
            List<Integer> idx = Utils.getIndexOfColumnsByConditions(conditions, columns);
            List<String> type = idx.stream().map(indice -> getColumns().get(indice).getType()).toList();
            res = res.parallelStream()
                    .filter(list -> validate(list, conditions, idx, type))
                    .collect(Collectors.toList());
        }
        return res;
    }

    private List<List<Object>> filterRowsWithIndexedColumn(List<List<Object>> res, List<Condition> conditions, List<Column> columnList) {
        // Vérifie d'abord si les conditions sont sur des colonnes indéxé et que la conditon est bien "="
        List<Condition> conditionsOnIndexedColumn = conditions.stream().filter(condition -> isConditionOnIndexedColumn(condition, columnList)).collect(Collectors.toList());

        if (!conditionsOnIndexedColumn.isEmpty()) { // Si y a des conditions sur des colonnes indéxées alors utilisent directement l'index pour récuperer les index des lignes qui correspondent
            // Récupérer les index de la première condition sur une colonne indéxé
            int idxColumn = Utils.getIndexOfColumnByCondition(conditionsOnIndexedColumn.get(0), columnList);
            TIntArrayList idxRows = new TIntArrayList();
            TIntArrayList listOfIndex = columnList.get(idxColumn).getIndex().getIndexFromValue(Utils.cast(conditionsOnIndexedColumn.get(0).getValue(), columnList.get(idxColumn).getType()));
            if (listOfIndex != null) {
                idxRows = listOfIndex;
            }
            // Parcours les autres conditions indéxés (si y en a) et
            // Récupère leurs index pour faire l'intersection des index de chaque résultat
            for (int i = 1; i < conditionsOnIndexedColumn.size(); i++) {
                int indexOfColumn = Utils.getIndexOfColumnByCondition(conditionsOnIndexedColumn.get(i), columnList);
                listOfIndex = columnList.get(indexOfColumn).getIndex().getIndexFromValue(Utils.cast(conditionsOnIndexedColumn.get(i).getValue(), columnList.get(indexOfColumn).getType()));
                if (listOfIndex != null) {
                    idxRows = Utils.intersectionSortedList(idxRows, listOfIndex);
                } else {
                    idxRows = new TIntArrayList();
                    break;
                }
            }
            // Va récupérer les lignes qui correspondent à l'intersection de tous les index
            List<List<Object>> resultat = new ArrayList<>(idxRows.size());
            for (int idx = 0; idx < idxRows.size(); idx++) {
                resultat.add(getRows().get(idxRows.get(idx)));
            }
            res = resultat;
            // Supprime les conditions qui ont déja été appliqué sur la liste de toutes nos conditions à appliquer
            conditions.removeAll(conditionsOnIndexedColumn);
        }
        return res;
    }

    public List<String> createIndexedColumns(List<List<Object>> rows) {
        List<String> columnsName = new ArrayList<>();
        if (!isIndexed) {
            List<Set<Object>> echantillon = new ArrayList<>(this.columns.size());
            for (Column column : this.columns) {
                echantillon.add(new HashSet<>());
            }
            for (List<Object> list : rows) {
                for (int j = 0; j < columns.size(); j++) {
                    Object obj = list.get(j);
                    if (obj != null)
                        echantillon.get(j).add(list.get(j));
                }
            }
            List<Integer> cardinalite = echantillon.stream().map(Set::size).collect(Collectors.toList());
            for(int i=0; i<cardinalite.size(); i++) {
                if(cardinalite.get(i) < rows.size()*0.05) {
                    columns.get(i).setIsIndex(true);
                    columns.get(i).setIndex(IndexFactory.create());
                    indexedColumns.add(columns.get(i));
                    columnsName.add(columns.get(i).getName());
                }
            }
            isIndexed = true;
        }
        return columnsName;
    }

    public boolean isConditionOnIndexedColumn(Condition condition, List<Column> columnList) {
        return columnList.stream().anyMatch( column -> column.isIndex() && column.getName().equalsIgnoreCase(condition.getNameColumn()) && condition.getOp().equals("=") );
    }

    public boolean deleteColumn(String nameColumn) {
        for(Column column : columns) {
            if(column.getName().equals(nameColumn)) {
                int idx = column.getNumber();
                for(List<Object> row : rows) {
                    row.remove(idx);
                }
                for(int i=idx+1; i<columns.size(); i++) {
                    columns.get(i).setNumber( columns.get(i).getNumber() - 1 );
                }
                columns.remove(idx);
                return true;
            }
        }
        return false;
    }

    public void addNewColumn(String nameColumn, String type, Object val) {
        columns.add(new Column(nameColumn, type, columns.size()));
        for(List<Object> row : rows) {
            row.add(val);
        }
    }
}
