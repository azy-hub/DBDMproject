package org.dant.api;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.schema.MessageType;
import org.dant.commons.Utils;
import org.dant.model.*;
import org.dant.rest.ForwardSlave1;
import org.dant.rest.ForwardSlave2;
import org.dant.select.ColumnSelected;
import org.dant.select.SelectMethod;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.resteasy.reactive.RestPath;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

@Path("/manager")
public class Controller {

    @RestClient
    ForwardSlave1 forwardSlave1;
    @RestClient
    ForwardSlave2 forwardSlave2;
    ExecutorService executor = Executors.newCachedThreadPool();


    @POST
    @Path("/parquet/fillTable/{tableName}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public String readParquet(@RestPath String tableName, File file) throws IOException {
        System.out.println("Start Parsing ...");
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Table table = DataBase.get().get(tableName);
        if (table == null)
            return "Table introuvable";
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        MessageType schema = footer.getFileMetaData().getSchema();
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        GroupRecordConverter groupRecordConverter = new GroupRecordConverter(schema);
        try (ParquetFileReader parquetFileReader = new ParquetFileReader( conf, path, footer)) {
            PageReadStore pages = null;
            while ((pages = parquetFileReader.readNextRowGroup()) != null ) {
                int rows = (int) pages.getRowCount();
                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, groupRecordConverter);
                int sizeBloc = 10000;
                BlockingQueue<Group> queue = new ArrayBlockingQueue<>(sizeBloc);
                Callable<Void> consumer = () -> {
                    int sizeSample = 10000;
                    List<List<Object>> listOfLists = new ArrayList<>(sizeBloc);
                    try {
                        int i=0;
                        while (i<rows/3) {
                            Group group = queue.take();
                            listOfLists.add(Utils.extractListFromGroup(group, table.getColumns()));
                            if (!table.getIsIndexed() && listOfLists.size() == sizeSample) {
                                List<String> columnsName = table.createIndexedColumns(listOfLists);
                                forwardSlave1.createIndexForTable(tableName,columnsName);
                                forwardSlave2.createIndexForTable(tableName,columnsName);
                            }
                            if(listOfLists.size() == sizeBloc) {
                                forwardSlave1.forwardRowsToTable(tableName,listOfLists);
                                listOfLists = new ArrayList<>(sizeBloc);
                            }
                            i++;
                        }
                        if(!listOfLists.isEmpty()) {
                            forwardSlave1.forwardRowsToTable(tableName,listOfLists);
                            listOfLists = new ArrayList<>(sizeBloc);
                        }
                        i=0;
                        while (i<rows/3) {
                            Group group = queue.take();
                            listOfLists.add(Utils.extractListFromGroup(group, table.getColumns()));
                            if(listOfLists.size() == sizeBloc) {
                                forwardSlave2.forwardRowsToTable(tableName,listOfLists);
                                listOfLists = new ArrayList<>(sizeBloc);
                            }
                            i++;
                        }
                        if(!listOfLists.isEmpty()) {
                            forwardSlave2.forwardRowsToTable(tableName,listOfLists);
                        }
                        i=0;
                        table.getRows().ensureCapacity(table.getRows().size() + (rows/3));
                        while (i<rows/3) {
                            Group group = queue.take();
                            table.addRow(Utils.extractListFromGroup(group, table.getColumns()));
                            i++;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                };
                Future<Void> consumerFuture = executor.submit(consumer);
                try {
                    for (int row = 0; row < rows; row++) {
                        Group group = recordReader.read();
                        queue.put(group);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                consumerFuture.get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long endTime = System.currentTimeMillis();
        return "Temps d'exécution : " + (endTime - startTime) + " ms";
    }

    @GET
    @Path("/select")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<List<Object>> getTableContent(SelectMethod selectMethod) {
        Table table = DataBase.get().get(selectMethod.getFROM());
        if (table == null) {
            System.out.println("La table avec le nom " + selectMethod.getFROM() + " n'a pas été trouvée.");
            throw new NotFoundException("La table avec le nom " + selectMethod.getFROM() + " n'a pas été trouvée.");
        }
        if (!table.checkSelectMethod(selectMethod)) {
            throw new NotFoundException("Params error");
        }
        CompletionStage<List<List<Object>>> future1 = forwardSlave1.getContent(selectMethod).thenApply( rows -> Utils.castResponseFromNode(table, selectMethod, rows));
        CompletionStage<List<List<Object>>> future2 = forwardSlave2.getContent(selectMethod).thenApply( rows -> Utils.castResponseFromNode(table, selectMethod, rows));

        List<List<Object>> res = table.select(selectMethod);

        res.addAll(future1.toCompletableFuture().join());
        res.addAll(future2.toCompletableFuture().join());

        List<ColumnSelected> aggregats = selectMethod.getAGGREGAT();
        if (aggregats != null && !aggregats.isEmpty()) {
            if ( selectMethod.getGROUPBY() != null && !selectMethod.getGROUPBY().isEmpty()) {
                res = Utils.aggregateResultNodesGroupBy(res, table, selectMethod);
            } else {
                res = Utils.aggregateResultNodes(res, table, selectMethod);
            }
        }
        return res;
    }

    @POST
    @Path("/insertRows/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String insertRows(@RestPath String tableName, List<List<Object>> listArgs) {
        Table table = DataBase.get().get(tableName);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + tableName + " n'a pas été trouvée.");
        forwardSlave1.forwardRowsToTable(tableName,listArgs.subList(0, listArgs.size()/3));
        forwardSlave2.forwardRowsToTable(tableName,listArgs.subList(listArgs.size()/3, 2*listArgs.size()/3));
        table.addAllRows(listArgs.subList(2*listArgs.size()/3, listArgs.size()).parallelStream().map( list -> Utils.castRow(list, table.getColumns())).toList());
        return "Rows added successfully !";
    }

}