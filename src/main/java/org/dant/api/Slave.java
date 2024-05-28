package org.dant.api;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.dant.commons.Utils;
import org.dant.model.*;
import org.dant.select.SelectMethod;
import org.jboss.resteasy.reactive.RestPath;

import java.io.File;
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
        System.out.println("Select FROM "+selectMethod.getFROM());
        List<List<Object>> res = DataBase.get().get(selectMethod.getFROM()).select(selectMethod);
        return res;
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
        System.out.println("Rows received !");
        Table table = DataBase.get().get(tableName);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + tableName + " n'a pas été trouvée.");
        table.addAllRows(listArgs.parallelStream().map( list -> Utils.castRow(list,table.getColumns())).collect(Collectors.toList()));
        System.out.println(listArgs.size()+" rows added !");
    }

}