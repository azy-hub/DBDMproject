package org.dant.rest;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.dant.model.Column;
import org.dant.select.SelectMethod;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface ForwardSlave {

    @POST
    @Path("/insertRows/{tableName}")
    CompletionStage<Void> forwardRowsToTable(@PathParam("tableName") String tableName, List<List<Object>> rows);

    @POST
    @Path("/createTable/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    CompletionStage<Void> createTable(@PathParam("tableName") String tableName, List<Column> listColumns);

    @POST
    @Path("/select")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    CompletionStage<List<List<Object>>> getContent(SelectMethod selectMethod);

    @POST
    @Path("/indexTable/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    CompletionStage<Void> createIndexForTable(@PathParam("tableName") String tableName, List<String> columnsName);

    @POST
    @Path("/deleteColumn")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    CompletionStage<Boolean> deleteColumn(@QueryParam("tableName") String tableName, @QueryParam("nameColumn") String nameColumn);

    @POST
    @Path("/addColumn")
    @Consumes(MediaType.APPLICATION_JSON)
    CompletionStage<Void> addColumn(@QueryParam("tableName") String tableName, @QueryParam("nameColumn") String nameColumn,
                                    @QueryParam("type") String type, @QueryParam("defaultValue") String defaultValue);


}
