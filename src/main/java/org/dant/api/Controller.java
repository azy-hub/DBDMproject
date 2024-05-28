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
import org.dant.commons.Forwarder;
import org.dant.commons.TypeDB;
import org.dant.commons.Utils;
import org.dant.model.*;
import org.dant.select.ColumnSelected;
import org.dant.select.Having;
import org.dant.select.SelectMethod;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestPath;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Path("/v1")
public class Controller {

    @ConfigProperty(name = "address.ip.1")
    String addressIp1;
    @ConfigProperty(name = "address.ip.2")
    String addressIp2;

    @POST
    @Path("/parquet/createTable/{tableName}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public String parseParquet(@RestPath String tableName, InputStream inputStream) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs;
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("temp.parquet");
        try {
            fs = FileSystem.get(conf);
            try (FSDataOutputStream outputStream = fs.create(path)) {
                IOUtils.copy(inputStream, outputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(path, conf);
            try (ParquetFileReader parquetFileReader = ParquetFileReader.open(hadoopInputFile)) {
                ParquetMetadata parquetMetadata = parquetFileReader.getFooter();
                MessageType parquetSchema = parquetMetadata.getFileMetaData().getSchema();
                List<Column> columns = new ArrayList<>();
                int i=0;
                for (ColumnDescriptor columnDescriptor : parquetSchema.getColumns()) {
                    columns.add(new Column(columnDescriptor.getPrimitiveType().getName(), columnDescriptor.getPrimitiveType().getPrimitiveTypeName().toString(), i++));
                }
                createTable(tableName,columns);
                return "Table created successfully";
            }
        } catch (IOException e) {
            return "error while creating table. "+e.getMessage();
        } finally {
            try {
                fs.delete(path, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @POST
    @Path("/parquet/fillTable/{tableName}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public String readParquet(@RestPath String tableName, File file) throws IOException {
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
                ExecutorService executorService = Executors.newFixedThreadPool(2);
                List<List<Object>> listOfList1 = new ArrayList<>(rows/3);
                for (int row = 0; row < rows/3; row++) {
                    listOfList1.add(Utils.extractListFromGroup(recordReader.read(), table.getColumns()));
                }
                Future<String> future1 = executorService.submit( () -> Forwarder.forwardRowsToTable(addressIp1,tableName, listOfList1));
                List<List<Object>> listOfList2 = new ArrayList<>(rows/3);
                for (int row = 0; row < rows/3; row++) {
                    listOfList2.add(Utils.extractListFromGroup(recordReader.read(), table.getColumns()));
                }
                Future<String> future2 = executorService.submit( () -> Forwarder.forwardRowsToTable(addressIp2,tableName, listOfList2));
                List<List<Object>> listOfList3 = new ArrayList<>(rows/3);
                for (int row = 0; row < rows/3; row++) {
                    listOfList3.add(Utils.extractListFromGroup(recordReader.read(), table.getColumns()));
                }
                table.addAllRows(listOfList3);
                listOfList3 = null;
                executorService.shutdown();
                future1.get();
                future2.get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long endTime = System.currentTimeMillis();
        return "Temps d'exécution : " + (endTime - startTime) + " ms";
    }

    @GET
    @Path("/DataBase")
    public String getAllTables() {
        StringBuilder stringBuilder = new StringBuilder("Les Tables sont : \n");
        Map<String, Table> map = DataBase.get();
        for(String name : map.keySet()) {
            stringBuilder.append("\nTable ").append(name).append(" :\n");
            stringBuilder.append("Number of lignes : ").append(map.get(name).getRows().size()).append("\n");
            List<Column> columns = map.get(name).getColumns();
            for( Column c : columns ) {
                stringBuilder.append("Colonne : ").append(c.getName()).append(", Type : ").append(c.getType()).append("\n");
            }
        }
        return stringBuilder.toString();
    }

    @POST
    @Path("/createTable/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public String createTable(@RestPath String tableName,List<Column> listColumns) {
        if (DataBase.get().containsKey(tableName)) {
            return "Table already exists with name : "+tableName;
        }
        if( listColumns.isEmpty() )
            return "Columns are empty";
        else
            new Table(tableName, listColumns);
        Forwarder.forwardCreateTable(addressIp1,tableName,listColumns);
        Forwarder.forwardCreateTable(addressIp2,tableName,listColumns);
        return "Table created successfully";
    }

    @GET
    @Path("/select")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<List<Object>> getTableContent(SelectMethod selectMethod) {
        Table table = DataBase.get().get(selectMethod.getFROM());

        if (table == null)
            throw new NotFoundException("La table avec le nom " + selectMethod.getFROM() + " n'a pas été trouvée.");

        if (!table.checkSelectMethod(selectMethod))
            throw new NotFoundException("Params error");

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<List<List<Object>>> future1 = executorService.submit(new Callable<List<List<Object>>>() {
            @Override
            public List<List<Object>> call() throws Exception {
                return Forwarder.forwardSelect(addressIp1,selectMethod);
            }
        });
        Future<List<List<Object>>> future2 = executorService.submit(new Callable<List<List<Object>>>() {
            @Override
            public List<List<Object>> call() throws Exception {
                return Forwarder.forwardSelect(addressIp2,selectMethod);
            }
        });

        List<List<Object>> res = table.select(selectMethod);
        try {
            res.addAll(future1.get());
            res.addAll(future2.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        executorService.shutdown();

        List<ColumnSelected> aggregats = selectMethod.getAGGREGAT();
        if (aggregats != null && !aggregats.isEmpty()) {

            if (selectMethod.getGROUPBY() != null) {
                // appliquer le groupBy pour tout mettre dans une Map (chaque valeur associé aux lignes de la table qui lui correspondent)
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
                        if (columnSelected.getTypeAggregat().equalsIgnoreCase("COUNT")) {
                            columnSelected.setTypeAggregat("SUM");
                            tmp.add(columnSelected.applyAggregat(list, idxOfAggregat + 1, TypeDB.INT));
                            columnSelected.setTypeAggregat("COUNT");
                        } else if (columnSelected.getTypeAggregat().equalsIgnoreCase("AVG")) {
                            tmp.add(columnSelected.applyAggregat(list, idxOfAggregat + 1, TypeDB.DOUBLE));
                        } else {
                            tmp.add(columnSelected.applyAggregat(list, idxOfAggregat + 1, table.getColumnByName(columnSelected.getNameColumn()).getType()));
                        }

                        if( !(selectMethod.getHAVING() == null || selectMethod.getHAVING().isEmpty()) ) {
                            for(Having having : selectMethod.getHAVING()) {
                                if( having.getAggregate().equals(columnSelected) && !having.checkCondition(tmp,idxOfAggregat+1,table.getColumnByName(columnSelected.getNameColumn()).getType()) ) {
                                    havingBool = false;
                                    break;
                                }
                            }
                        }

                    }
                    if(havingBool)
                        resultat.add(tmp);
                });
                res = resultat;
            } else {
                List<List<Object>> resTmp = new ArrayList<>();
                List<Object> tmp = new ArrayList<>(aggregats.size());
                boolean havingBool = true;
                for (int idxOfAggregat = 0; idxOfAggregat < aggregats.size(); idxOfAggregat++) {
                    ColumnSelected columnSelected = aggregats.get(idxOfAggregat);
                    if (columnSelected.getTypeAggregat().equals("COUNT")) {
                        columnSelected.setTypeAggregat("SUM");
                        tmp.add(columnSelected.applyAggregat(res, idxOfAggregat, TypeDB.INT));
                        columnSelected.setTypeAggregat("COUNT");
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
                res = resTmp;
            }
        }
        return res;
    }

    @POST
    @Path("/insertOneRow/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void insertOneRow(@RestPath String name, List<Object> args) {
        Table table = DataBase.get().get(name);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + name + " n'a pas été trouvée.");
        if(args.size() != table.getColumns().size()) {
            throw new IllegalArgumentException("Nombre d'argument incorrect.");
        }
        table.addRow(args);
    }

    @POST
    @Path("/insertRows/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String insertRows(@RestPath String tableName, List<List<Object>> listArgs) {
        Table table = DataBase.get().get(tableName);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + tableName + " n'a pas été trouvée.");

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit( ()-> Forwarder.forwardRowsToTable(addressIp1,tableName,listArgs.subList(0, listArgs.size()/3)) );
        executorService.submit( ()-> Forwarder.forwardRowsToTable(addressIp2,tableName,listArgs.subList(listArgs.size()/3, 2*listArgs.size()/3)) );
        table.addAllRows(listArgs.subList(2*listArgs.size()/3, listArgs.size()).parallelStream().map( list -> Utils.castRow(list, table.getColumns())).collect(Collectors.toList()));
        executorService.shutdown();
        return "Rows added successfully !";
    }
}