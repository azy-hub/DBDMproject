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
import org.dant.commons.TypeDB;
import org.dant.commons.Utils;
import org.dant.index.IndexFactory;
import org.dant.model.*;
import org.dant.select.SelectMethod;

import java.io.File;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Path("/slave")
public class Slave {

    @POST
    @Path("/createTable/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createTable(@PathParam("tableName") String tableName, List<Column> listColumns) {
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
        System.out.println("Return "+res.size()+" rows !");
        return res;
    }

    @POST
    @Path("/insertRows/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void insertRows(@PathParam("tableName") String tableName, List<List<Object>> listArgs) {
        System.out.println("Rows received !");
        Table table = DataBase.get().get(tableName);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + tableName + " n'a pas été trouvée.");
        table.addAllRows(listArgs.parallelStream().map( list -> Utils.castRow(list,table.getColumns())).collect(Collectors.toList()));
        System.out.println(listArgs.size()+" rows added !");
    }

    @POST
    @Path("/indexTable/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createIndexForTable(@PathParam("tableName") String tableName, List<String> columnsName) {
        System.out.println("Index received !");
        Table table = DataBase.get().get(tableName);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + tableName + " n'a pas été trouvée.");
        for(Column column : table.getColumns()) {
            if(columnsName.contains(column.getName())) {
                column.setIsIndex(true);
                column.setIndex(IndexFactory.create());
                table.getIndexedColumns().add(column);
            }
        }
    }

    @POST
    @Path("/deleteColumn")
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean deleteColumn(@QueryParam("tableName") String tableName, @QueryParam("nameColumn") String nameColumn) {
        Table table = DataBase.get().get(tableName);
        if(table == null)
            return false;
        boolean deleted = table.deleteColumn(nameColumn);
        return deleted;
    }

    @POST
    @Path("/addColumn")
    @Consumes(MediaType.APPLICATION_JSON)
    public void addColumn(@QueryParam("tableName") String tableName, @QueryParam("nameColumn") String nameColumn,
                            @QueryParam("type") String type, @QueryParam("defaultValue") String defaultValue) {
        Table table = DataBase.get().get(tableName);
        if(table == null)
            return;
        if( !List.of(TypeDB.STRING, TypeDB.LONG, TypeDB.INT, TypeDB.DOUBLE, TypeDB.SHORT, TypeDB.BYTE).contains(type))
            return;
        Object val = null;
        if (defaultValue != null) {
            try {
                switch (type) {
                    case TypeDB.DOUBLE:
                        val = Double.parseDouble(defaultValue);
                        break;
                    case TypeDB.STRING:
                        val = defaultValue;
                        break;
                    case TypeDB.LONG:
                        val = Long.parseLong(defaultValue);
                        break;
                    case TypeDB.INT:
                        val = Integer.parseInt(defaultValue);
                        break;
                    case TypeDB.SHORT:
                        val = Short.parseShort(defaultValue);
                        break;
                    case TypeDB.BYTE:
                        val = Byte.parseByte(defaultValue);
                        break;
                    default:
                        return;
                }
            } catch (ClassCastException | ArithmeticException e) {
                return;
            }
        }
        table.addNewColumn(nameColumn, type, val);
    }

}