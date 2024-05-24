package org.dant.model;

import gnu.trove.TIntArrayList;
import org.dant.commons.Utils;
import org.dant.commons.SpinLock;
import org.dant.commons.TypeDB;
import org.dant.index.IndexFactory;
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
        rows.add(row);
    }

    public void addAllRows(List<List<Object>> lignes) {
        if(this.rows.isEmpty()) {
            createIndexedColumns(lignes);
        }
        int nbThread = 4;
        final int[] rowsIdx = {rows.size()};
        List<Thread> threadList = new ArrayList<>(nbThread);
        for(int i = 0; i<nbThread; i++) {
            int finalI = i;
            Runnable runnable = () -> {
                int index = rowsIdx[0];
                for(List<Object> row : lignes) {
                    for(int idx=finalI*(indexedColumns.size()); idx<(finalI+1)*(indexedColumns.size()/nbThread); idx++) {
                        indexedColumns.get(idx).getIndex().addIndex(row.get(columns.indexOf(indexedColumns.get(idx))), index );
                    }
                    index++;
                }
            };
            Thread thread = new Thread(runnable);
            threadList.add(thread);
            thread.start();
        }
        rows.addAll(lignes);
        for(Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
        // Récupérer la liste des colonnes qu'on séléctionne
        List<Column> columnList = getColumnsByNames(selectMethod.getSELECT());
        List<List<Object>> res = rows;

        // Récupère les conditions qui ont été soumise dans le WHERE
        List<Condition> conditions = selectMethod.getWHERE();
        int nbConditions = (conditions != null) ? conditions.size() : 0;

        if (conditions!= null && !conditions.isEmpty()) {
            // Filtre par les conditions qui sont sur des colonnes indéxé
            res = filterRowsWithIndexedColumn(res, conditions, columnList);

            // Filtre par les conditions qui sont pas
            res = filterRowsWithoutIndexedColumn(res, conditions);

        }


        // Vérifie si un groupBy et un aggrégat ont été demandé dans la requete SELECT
        if ( selectMethod.getAGGREGAT() != null && !selectMethod.getAGGREGAT().isEmpty()) {
            if ( selectMethod.getGROUPBY() != null && !selectMethod.getGROUPBY().isEmpty()) {
                int idxOfColumnGroupBy = Utils.getIdxColumnByName(columns, selectMethod.getGROUPBY()); // Trouve l'index de la colonne à regrouper parmis les colonnes selectionnées
                // si le groupBy est appliqué sur un index et que aucune condition de filtre n'a été appliqué alors on peut directement récupérer les valeurs du groupBy par l'index
                if ( columnList.get(idxOfColumnGroupBy).isIndex() && nbConditions == 0) {

                    List<List<Object>> groupby = new ArrayList<>( columnList.get(idxOfColumnGroupBy).getIndex().getKeys().size() );
                    columnList.get(idxOfColumnGroupBy).getIndex().getKeys().parallelStream().forEach(key -> {
                        TIntArrayList idxRows = columnList.get(idxOfColumnGroupBy).getIndex().getIndexFromValue(key);
                        List<List<Object>> resultat = new ArrayList<>(idxRows.size());
                        for (int idx = 0; idx < idxRows.size(); idx++) {
                            resultat.add(getRows().get(idxRows.get(idx)));
                        }
                        List<Object> tmp = selectMethod.applyAllAggregats(columns, resultat);
                        tmp.add(0, key);
                        groupby.add(tmp);
                    });
                    res = groupby;

                }
                else {
                    System.out.println("group by sans index");
                    Map<Object, List<List<Object>>> groupBy = new HashMap<>();
                    // Parcours chaque ligne et regroupe chaque valeur avec les lignes qui lui correspondent
                    SpinLock groupByLock = new SpinLock();
                    res.parallelStream().forEach(list -> {
                        Object object = list.get(idxOfColumnGroupBy);
                        groupByLock.lock();
                        groupBy.computeIfAbsent(object, k -> new ArrayList<>()).add(list);
                        groupByLock.unlock();
                    });

                    // Parcours la map générer grace au regroupement et applique les aggrégats demandés
                    List<List<Object>> resultat = Collections.synchronizedList(new LinkedList<>());
                    groupBy.keySet().parallelStream().forEach( obj -> {
                        List<Object> tmp = selectMethod.applyAllAggregats(columns, groupBy.get(obj));
                        tmp.add(0, obj);
                        resultat.add( tmp );
                    });
                    res = resultat;

                }
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
            System.out.println("Condition avec index");
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
                    columns.get(i).setIndex(IndexFactory.create());
                    indexedColumns.add(columns.get(i));
                }
            }
        }
    }

    public boolean isConditionOnIndexedColumn(Condition condition, List<Column> columnList) {
        return columnList.stream().anyMatch( column -> column.isIndex() && column.getName().equals(condition.getNameColumn()) && condition.getOp().equals("=") );
    }

}
