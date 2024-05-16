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
            Object object = row.get(columns.indexOf(column));
            if(object != null) {
                column.getIndex().addIndex(object, idxRow);
            }
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
        //if ( !((selectMethod.getWHERE() == null) || selectMethod.getWHERE().stream().allMatch(this::checkCondition)))
        //    return false;
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

        // Récupérer de la base de données toutes les lignes avec seulement les colonnes souhaitées
        List<List<Object>> res = getRows().parallelStream()
                .map( row -> transform(row,columnList) )
                .collect(Collectors.toList());

        // Récupère les conditions qui ont été soumise dans le WHERE
        List<Condition> conditions = selectMethod.getWHERE();

        // Teste si y a des conditions à appliquer pour filtrer :
        if ( !(conditions == null || conditions.isEmpty()) ) {

            // Vérifie d'abord si les conditions sont sur des colonnes indéxé et que la conditon est bien "="
            List<Condition> conditionsOnIndexedColumn = conditions.stream().filter(condition -> isConditionOnIndexedColumn(condition, columnList)).collect(Collectors.toList());

            if (!conditionsOnIndexedColumn.isEmpty()) { // Si y a des conditions sur des colonnes indéxées alors utilisent directement l'index pour récuperer les index des lignes qui correspondent

                // Récupérer les index de la première condition sur une colonne indéxé
                int idxColumn = Utils.getIndexOfColumnByCondition(conditionsOnIndexedColumn.get(0), columnList);
                TIntArrayList idxRows = new TIntArrayList();
                TIntArrayList listOfIndex = columnList.get(idxColumn).getIndex().getIndexsFromValue(Utils.cast(conditionsOnIndexedColumn.get(0).getValue(), columnList.get(idxColumn).getType()));
                if (listOfIndex != null) {
                    idxRows = listOfIndex;
                }

                // Parcours les autres conditions indéxés (si y en a) et
                // Récupère leurs index pour faire l'intersection des index de chaque résultat
                for (int i = 1; i < conditionsOnIndexedColumn.size(); i++) {
                    int indexOfColumn = Utils.getIndexOfColumnByCondition(conditionsOnIndexedColumn.get(i), columnList);
                    listOfIndex = columnList.get(indexOfColumn).getIndex().getIndexsFromValue(conditionsOnIndexedColumn.get(i).getValue());
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
                    resultat.add(res.get(idxRows.get(idx)));
                }
                res = resultat;
                // Supprime les conditions qui ont déja été appliqué sur la liste de toutes nos conditions à appliquer
                conditions.removeAll(conditionsOnIndexedColumn);
            }
        }

        // Décompresse toutes les lignes en bytes vers de la VRAI DONNÉE
        res = res.parallelStream().map( list -> {
            List<Object> objectList = new ArrayList<>(list.size());
            for(int idx=0; idx<columnList.size(); idx++)
                objectList.add( columnList.get(idx).fromBytesToObject.apply(list.get(idx)) );
            return objectList;
        }).collect(Collectors.toList());

        // Vérifie si y a des conditions à traiter pour filtrer encore sur les lignes
        if ( !(conditions == null || conditions.isEmpty()) ) {
            List<Integer> idx = Utils.getIndexOfColumnsByConditions(conditions, columnList);
            List<String> type = idx.stream().map(indice -> columnList.get(indice).getType()).toList();
            res = res.parallelStream().filter(list -> validate(list, conditions, idx, type))
                    .collect(Collectors.toList());
        }


        // Vérifie si un groupBy et un aggrégat ont été demandé dans la requete SELECT
        if (selectMethod.getGROUPBY() != null && selectMethod.getAGGREGAT() != null && !selectMethod.getAGGREGAT().isEmpty()) {
            int idxOfColumnGroupBy = Utils.getIdxColumnByName(columnList, selectMethod.getGROUPBY()); // Trouve l'index de la colonne à regrouper parmis les colonnes selectionnées

            Map<Object, List<List<Object>>> groupBy = new HashMap<>();
            // TODO : Ajouter la récupération par l'index si le groupBy a été fait par une colonne indexé
            // Parcours chaque ligne et regroupe chaque valeur avec les lignes qui lui correspondent
            res.forEach(list -> {
                Object object = list.get(idxOfColumnGroupBy);
                groupBy.computeIfAbsent(object, k -> new ArrayList<>()).add(list);
            });

            // Parcours la map générer grace au regroupement et applique les aggrégats demandés
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
            List<Integer> cardinalite = echantillon.stream().map(Set::size).toList();
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
