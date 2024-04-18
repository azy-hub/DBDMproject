package org.dant;

import jakarta.annotation.Nullable;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import org.dant.model.Column;
import org.dant.model.Condition;
import org.dant.model.DataBase;
import org.dant.model.Table;
import org.jboss.resteasy.reactive.RestPath;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Path("/slave")
public class Slave {

    @POST
    @Path("/createTable/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createTable(@RestPath String name, List<Column> listColumns) {
        System.out.println("Create table "+name);
        if( listColumns.isEmpty() )
            new Table(name);
        else
            new Table(name, listColumns);
        System.out.println("Table created successfully");
    }

    @POST
    @Path("/select/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public List<List<Object>> getContent(@RestPath("tableName") String tableName,@Nullable List<Condition> conditions) {
        Table table = DataBase.get().get(tableName);
        List<List<Object>> res = new LinkedList<>();
        if (conditions == null || conditions.isEmpty()) {
            res = table.getRows();
        } else {
            List<Integer> idx = table.getIndexOfColumnsByConditions(conditions);
            List<String> type = idx.stream().map(indice -> table.getColumns().get(indice).getType()).toList();
            res = table.getRows().parallelStream().filter(list -> table.validate(list, conditions, idx, type)).collect(Collectors.toList());
        }
        return res;
    }

}