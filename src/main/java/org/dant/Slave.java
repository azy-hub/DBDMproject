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
    @Path("/select")
    @Consumes(MediaType.APPLICATION_JSON)
    public List<List<Object>> getContent(SelectMethod selectMethod) {
        return DataBase.get().get(selectMethod.getFROM()).select(selectMethod);
    }

    @POST
    @Path("/addRowToTable/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void addRowToTable(@RestPath String name, List<Object> args) {
        Table table = DataBase.get().get(name);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + name + " n'a pas été trouvée.");
        if(args.size() != table.getColumns().size())
            throw new NotFoundException("Nombre d'argument incorrect.");
        table.addRow(args);
    }

}