package org.dant.commons;

import gnu.trove.TIntArrayList;
import org.apache.parquet.example.data.Group;
import org.dant.model.Column;
import org.dant.commons.TypeDB;
import org.dant.model.Table;
import org.dant.select.ColumnSelected;
import org.dant.select.Condition;
import org.dant.select.Having;
import org.dant.select.SelectMethod;
import org.wildfly.common.lock.SpinLock;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Utils {

    public static List<Object> extractListFromGroup(Group group, List<Column> columns) {
        List<Object> list = new ArrayList<>(columns.size());
        for(Column column : columns) {
            list.add(column.extractFromGroup.apply(group));
        }
        return list;
    }

    public static List<Object> castRow(List<Object> args, List<Column> columns) {
        List<Object> list = new ArrayList<>(columns.size());
        for(int i=0; i<columns.size(); i++) {
            if (args.get(i) == null)
                list.add(null);
            else {
                list.add( columns.get(i).parseJson.apply(args.get(i)) );
            }
        }
        return list;
    }

    public static Object cast(Object object, String type) {
        return switch (type) {
            case TypeDB.DOUBLE -> ((BigDecimal) object).floatValue();
            case TypeDB.STRING -> object.toString();
            case TypeDB.LONG -> ((BigDecimal) object).longValue();
            case TypeDB.INT -> ((BigDecimal) object).intValue();
            case TypeDB.SHORT -> ((BigDecimal) object).shortValue();
            case TypeDB.BYTE -> ((BigDecimal) object).byteValue();
            default -> null;
        };
    }

    public static TIntArrayList intersectionSortedList(TIntArrayList list1, TIntArrayList list2) {
        TIntArrayList list = new TIntArrayList();
        int j=0;
        int k=0;
        while(j<list1.size() && k<list2.size()) {
            if( list1.getQuick(j) == list2.getQuick(k) ) {
                list.add(list1.get(j));
                j++;
                k++;
            }
            if( list1.getQuick(j) < (list2.getQuick(k)) ) {
                j++;
            } else {
                k++;
            }
        }
        return list;
    }

    public static int getIdxColumnByName(List<Column> columnList, String name) {
        for(Column column : columnList) {
            if (column.getName().equalsIgnoreCase(name)) {
                return columnList.indexOf(column);
            }
        }
        return -1;
    }

    public static int getIndexOfColumnByCondition (Condition condition, List<Column> columns) {
        for (Column column : columns) {
            if (condition.getNameColumn().equalsIgnoreCase(column.getName())) {
                return columns.indexOf(column);
            }
        }
        return -1;
    }

    public static List<Integer> getIndexOfColumnsByConditions(List<Condition> conditions, List<Column> colonnes) {
        return conditions.stream().map( condition -> Utils.getIndexOfColumnByCondition(condition,colonnes) ).toList();
    }

    public static List<List<Object>> castResponseFromNode(Table table, SelectMethod selectMethod, List<List<Object>> rows) {
        return rows.stream().map( row -> {
            if(selectMethod.getAGGREGAT()!=null && !selectMethod.getAGGREGAT().isEmpty()) {
                List<Object> newList = new ArrayList<>();
                if(selectMethod.getGROUPBY()!=null && !selectMethod.getGROUPBY().isEmpty()) {
                    Object object = row.get(0);
                    List<Object> groupby = new ArrayList<>((List<Object>) object);
                    for (String nameColumn : selectMethod.getGROUPBY()) {
                        if (table.getColumnByName(nameColumn).getType().equalsIgnoreCase(TypeDB.LONG)) {
                            int idx = selectMethod.getGROUPBY().indexOf(nameColumn);
                            Object tmp = groupby.get(idx);
                            tmp = (tmp instanceof Integer) ? ((Integer) tmp).longValue() : (Long) tmp;
                            groupby.remove(idx);
                            groupby.add(idx, tmp);
                        }
                        if (table.getColumnByName(nameColumn).getType().equalsIgnoreCase(TypeDB.DOUBLE)) {
                            int idx = selectMethod.getGROUPBY().indexOf(nameColumn);
                            Object tmp = groupby.get(idx);
                            tmp = (tmp instanceof Double) ? ((Double) tmp).floatValue() : (Float) tmp;
                            groupby.remove(idx);
                            groupby.add(idx, tmp);
                        }
                    }
                    row.remove(0);
                    newList.add(0, groupby);
                }
                for (int idxAggregate=0; idxAggregate<selectMethod.getAGGREGAT().size(); idxAggregate++){
                    Object object = row.get(idxAggregate);
                    switch (selectMethod.getAGGREGAT().get(idxAggregate).getTypeAggregat()) {
                        case "AVG":
                            object = (object instanceof Double) ? (Double)object : (object instanceof Integer) ? ((Integer)object).doubleValue() : ((Float)object).doubleValue();
                            break;
                        case "SUM","MAX","MIN":
                            if (table.getColumnByName(selectMethod.getAGGREGAT().get(idxAggregate).getNameColumn()).getType().equalsIgnoreCase(TypeDB.LONG)) {
                                object = (object instanceof Integer) ? ((Integer) object).longValue() : (Long)object;
                            }
                            if (table.getColumnByName(selectMethod.getAGGREGAT().get(idxAggregate).getNameColumn()).getType().equalsIgnoreCase(TypeDB.DOUBLE)) {
                                object = (object instanceof Double) ? (Double)object : (object instanceof Integer) ? ((Integer)object).doubleValue() : ((Float)object).doubleValue();
                            }
                            break;
                        default:
                            break;
                    }
                    newList.add(object);
                }
                return newList;
            } else {
                return row;
            }
        }).toList();
    }

    public static List<List<Object>> aggregateResultNodesGroupBy(List<List<Object>> res, Table table, SelectMethod selectMethod) {
        // appliquer le groupBy pour tout mettre dans une Map (chaque valeur associé aux lignes de la table qui lui correspondent)
        List<ColumnSelected> aggregats = selectMethod.getAGGREGAT();
        Map<Object, List<List<Object>>> groupBy = new HashMap<>();
        res.forEach(list -> {
            Object object = list.get(0);
            groupBy.computeIfAbsent(object, k -> new ArrayList<>()).add(list);
        });

        List<List<Object>> resultat = new ArrayList<>(groupBy.keySet().size());
        groupBy.forEach((obj, list) -> {
            List<Object> tmp = new ArrayList<>(1 + aggregats.size());
            tmp.add(obj);
            boolean havingBool = true;
            for (int idxOfAggregat = 0; idxOfAggregat < aggregats.size(); idxOfAggregat++) {
                ColumnSelected columnSelected = aggregats.get(idxOfAggregat);
                int finalIdxOfAggregat = idxOfAggregat;
                if (columnSelected.getTypeAggregat().equalsIgnoreCase("COUNT")) {
                    tmp.add( list.stream().mapToInt(row -> (int)row.get(finalIdxOfAggregat +1)).sum() );
                } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("AVG")) {
                    tmp.add(list.stream().mapToDouble( row -> (double)row.get(finalIdxOfAggregat +1)).sum() / (double) list.size());
                } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("SUM") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.LONG)) {
                    tmp.add( list.stream().mapToLong( row -> (Long)row.get(finalIdxOfAggregat +1)).sum());
                } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("SUM") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.DOUBLE)) {
                    tmp.add( list.stream().mapToDouble( row -> (Double)row.get(finalIdxOfAggregat +1) ).sum());
                } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("MAX") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.LONG)) {
                    tmp.add( list.stream().mapToLong( row -> (Long)row.get(finalIdxOfAggregat +1)).max());
                } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("MAX") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.DOUBLE)) {
                    tmp.add( list.stream().mapToDouble( row -> (Double)row.get(finalIdxOfAggregat +1) ).max());
                } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("MIN") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.LONG)) {
                    tmp.add( list.stream().mapToLong( row -> (Long)row.get(finalIdxOfAggregat +1)).min());
                } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("MIN") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.DOUBLE)) {
                    tmp.add( list.stream().mapToDouble( row -> (Double)row.get(finalIdxOfAggregat +1) ).min());
                } else {
                    tmp.add(columnSelected.applyAggregat(list, idxOfAggregat + 1, table.getColumnByName(columnSelected.getNameColumn()).getType()));
                }

                if( !(selectMethod.getHAVING() == null || selectMethod.getHAVING().isEmpty()) ) {
                    for(Having having : selectMethod.getHAVING()) {
                        if( having.getAggregate().getNameColumn().equals("*")) {
                            if( having.getAggregate().equals(columnSelected) && !having.checkHaving(tmp,idxOfAggregat+1,TypeDB.INT) ) {
                                havingBool = false;
                                break;
                            }
                        } else {
                            if (having.getAggregate().equals(columnSelected) && !having.checkHaving(tmp, idxOfAggregat + 1, table.getColumnByName(columnSelected.getNameColumn()).getType())) {
                                havingBool = false;
                                break;
                            }
                        }
                    }
                }

            }
            if(havingBool)
                resultat.add(tmp);
        });
        return resultat;
    }

    public static List<List<Object>> aggregateResultNodes(List<List<Object>> res, Table table, SelectMethod selectMethod) {
        List<ColumnSelected> aggregats = selectMethod.getAGGREGAT();
        List<List<Object>> resTmp = new ArrayList<>();
        List<Object> tmp = new ArrayList<>(aggregats.size());
        boolean havingBool = true;
        for (int idxOfAggregat = 0; idxOfAggregat < aggregats.size(); idxOfAggregat++) {
            ColumnSelected columnSelected = aggregats.get(idxOfAggregat);
            int finalIdxOfAggregat = idxOfAggregat;
            if (columnSelected.getTypeAggregat().equals("COUNT")) {
                tmp.add(res.stream().mapToInt(row -> (int)row.get(finalIdxOfAggregat)).sum());
            } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("AVG")) {
                tmp.add(res.stream().mapToDouble( row -> (double)row.get(finalIdxOfAggregat)).sum() / (double) res.size());
            } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("SUM") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.LONG)) {
                tmp.add( res.stream().mapToLong( row -> (Long)row.get(finalIdxOfAggregat)).sum());
            } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("SUM") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.DOUBLE)) {
                tmp.add( res.stream().mapToDouble( row -> (Double)row.get(finalIdxOfAggregat)).sum());
            } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("MAX") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.LONG)) {
                tmp.add( res.stream().mapToLong( row -> (Long)row.get(finalIdxOfAggregat)).max());
            } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("MAX") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.DOUBLE)) {
                tmp.add( res.stream().mapToDouble( row -> (Double)row.get(finalIdxOfAggregat) ).max());
            } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("MIN") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.LONG)) {
                tmp.add( res.stream().mapToLong( row -> (Long)row.get(finalIdxOfAggregat)).min());
            } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("MIN") && table.getColumnByName(columnSelected.getNameColumn()).getType().equals(TypeDB.DOUBLE)) {
                tmp.add( res.stream().mapToDouble( row -> (Double)row.get(finalIdxOfAggregat) ).min());
            } else {
                tmp.add(columnSelected.applyAggregat(res, idxOfAggregat, table.getColumnByName(columnSelected.getNameColumn()).getType()));
            }

            if( !(selectMethod.getHAVING() == null || selectMethod.getHAVING().isEmpty()) ) {
                for(Having having : selectMethod.getHAVING()) {
                    if( having.getAggregate().equals(columnSelected) && !having.getCondition().checkCondition(tmp,idxOfAggregat,table.getColumnByName(columnSelected.getNameColumn()).getType()) ) {
                        havingBool = false;
                        break;
                    }
                }
            }
        }
        if (havingBool)
            resTmp.add(tmp);
        return resTmp;
    }


}
