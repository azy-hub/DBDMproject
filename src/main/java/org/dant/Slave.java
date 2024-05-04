package org.dant;

import jakarta.annotation.Nullable;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.dant.model.*;
import org.jboss.resteasy.reactive.RestPath;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Path("/slave")
public class Slave {

    @POST
    @Path("/createTable/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createTable(@RestPath String tableName, List<Column> listColumns) {
        if (DataBase.get().containsKey(tableName)) {
            System.out.println( "Table already exists with name : "+tableName);
            return;
        }
        if( listColumns.isEmpty() ) {
            System.out.println("Columns are empty");
            return;
        }
        new Table(tableName, listColumns);
        System.out.println("Table created successfully");
    }

    @POST
    @Path("/select")
    @Consumes(MediaType.APPLICATION_JSON)
    public List<List<Object>> getContent(SelectMethod selectMethod) {
        System.out.println("Select "+selectMethod.getSELECT()+", FROM "+selectMethod.getFROM());
        return DataBase.get().get(selectMethod.getFROM()).select(selectMethod);
    }

    @POST
    @Path("/insertOneRow/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void insertOneRow(@RestPath String name, List<Object> args) {
        Table table = DataBase.get().get(name);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + name + " n'a pas été trouvée.");
        if(args.size() != table.getColumns().size())
            throw new NotFoundException("Nombre d'argument incorrect.");
        table.addRow(args);
    }

    @POST
    @Path("/insertRows/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void insertRows(@RestPath String tableName, List<List<Object>> listArgs) {
        Table table = DataBase.get().get(tableName);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + tableName + " n'a pas été trouvée.");
        table.addAllRows(listArgs.stream().map( list -> table.castRow(list)).collect(Collectors.toList()));
        System.out.println(listArgs.size()+" rows added !");
    }

}