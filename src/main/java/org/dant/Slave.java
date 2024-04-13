package org.dant;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import org.dant.model.Column;
import org.dant.model.Table;
import org.jboss.resteasy.reactive.RestPath;

import java.util.List;

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

}