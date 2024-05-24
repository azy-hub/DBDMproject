package org.dant.api;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.RecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.schema.MessageType;
import org.dant.commons.TypeDB;
import org.dant.commons.Utils;
import org.dant.commons.SpinLock;
import org.dant.model.*;
import org.dant.select.Aggregat;
import org.dant.select.SelectMethod;
import org.jboss.resteasy.reactive.RestPath;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Path("/v1")
public class Controller {

    public static final String addressIp1 = "192.168.6.21";
    public static final String addressIp2 = "192.168.6.117";

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
                for (ColumnDescriptor columnDescriptor : parquetSchema.getColumns()) {
                    columns.add(new Column(columnDescriptor.getPrimitiveType().getName(), columnDescriptor.getPrimitiveType().getPrimitiveTypeName().toString()));
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
    public void readParquet(@RestPath String tableName, File file) throws IOException {
        Configuration conf = new Configuration();
        Table table = DataBase.get().get(tableName);
        if (table == null)
            return;
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
        try (ParquetFileReader parquetFileReader = new ParquetFileReader(HadoopInputFile.fromPath(path, new Configuration()), ParquetReadOptions.builder().build())) {
            ParquetMetadata footer = parquetFileReader.getFooter();
            MessageType schema = footer.getFileMetaData().getSchema();
            PageReadStore pages;

            while ((pages = parquetFileReader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                RecordReader<Group> recordReader = new ColumnIOFactory().getColumnIO(schema).getRecordReader(pages, new GroupRecordConverter(schema));
                final SpinLock lock = new SpinLock();

                //ExecutorService executorService = Executors.newFixedThreadPool(3);

                /*
                Future<String> forwarder1 =  executorService.submit( () -> {
                    List<List<Object>> listOfList = new LinkedList<>();
                    for(long row=0; row<rows/3 ;row++) {
                        listOfList.add(Utils.extractListFromGroup(recordReader.read(), table.getColumns()));
                    }
                    Forwarder.forwardRowsToTable(addressIp1,tableName,listOfList);
                    return "fini";
                });
                Future<String> forwarder2 =  executorService.submit( () -> {
                    List<List<Object>> listOfList = new LinkedList<>();
                    for(long row=0; row<rows/3 ;row++) {
                        listOfList.add(Utils.extractListFromGroup(recordReader.read(), table.getColumns()));
                    }
                    Forwarder.forwardRowsToTable(addressIp2,tableName,listOfList);
                    return "fini";
                });*/
                List<List<Object>> listOfList = new ArrayList<>((int) rows);
                for (long row = 0; row < rows; row++) {
                    listOfList.add(Utils.extractListFromGroup(recordReader.read(), table.getColumns()));
                }
                table.addAllRows(listOfList);
                //executorService.shutdown();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("FINI !");
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
        //Forwarder.forwardCreateTable(addressIp1,tableName,listColumns);
        //Forwarder.forwardCreateTable(addressIp2,tableName,listColumns);
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

        /*ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<List<List<Object>>> future1 = executorService.submit(new Callable<List<List<Object>>>() {
            @Override
            public List<List<Object>> call() throws Exception {
                return Forwarder.forwardGetTableContent(addressIp1,selectMethod);
            }
        });
        Future<List<List<Object>>> future2 = executorService.submit(new Callable<List<List<Object>>>() {
            @Override
            public List<List<Object>> call() throws Exception {
                return Forwarder.forwardGetTableContent(addressIp2,selectMethod);
            }
        });*/

        List<List<Object>> res = table.select(selectMethod);
        /*try {
            res.addAll(future1.get());
            res.addAll(future2.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        executorService.shutdown();*/

        if (selectMethod.getAGGREGAT() != null && !selectMethod.getAGGREGAT().isEmpty()) {

            if (selectMethod.getGROUPBY() != null) {
                // appliquer le groupBy pour tout mettre dans une Map (chaque valeur associé aux lignes de la table qui lui correspondent)
                Map<Object, List<List<Object>>> groupBy = new HashMap<>();
                res.forEach(list -> {
                    Object object = list.get(0);
                    groupBy.computeIfAbsent(object, k -> new ArrayList<>()).add(list);
                });

                List<List<Object>> resultat = new ArrayList<>(groupBy.keySet().size());
                groupBy.forEach((obj, list) -> {
                    List<Object> tmp = new ArrayList<>(1 + selectMethod.getAGGREGAT().size());
                    tmp.add(obj);
                    boolean havingBool = true;
                    for (int idxOfAggregat = 0; idxOfAggregat < selectMethod.getAGGREGAT().size(); idxOfAggregat++) {
                        Aggregat aggregat = selectMethod.getAGGREGAT().get(idxOfAggregat);
                        if (aggregat.getTypeAggregat().equals("COUNT")) {
                            aggregat.setTypeAggregat("SUM");
                            tmp.add(aggregat.applyAggregat(list, idxOfAggregat + 1, TypeDB.INT));
                            aggregat.setTypeAggregat("COUNT");
                            if (aggregat.getHAVING() != null && !aggregat.getHAVING().checkCondition(tmp, idxOfAggregat + 1, TypeDB.INT)) {
                                havingBool = false;
                                break;
                            }
                        } else {
                            tmp.add(aggregat.applyAggregat(list, idxOfAggregat + 1, table.getColumnByName(aggregat.getNameColumn()).getType()));
                            if (aggregat.getHAVING() != null && !aggregat.getHAVING().checkCondition(tmp, idxOfAggregat + 1, table.getColumnByName(aggregat.getNameColumn()).getType())) {
                                havingBool = false;
                                break;
                            }
                        }
                    }
                    if (havingBool)
                        resultat.add(tmp);
                });
                res = resultat;
            } else {
                List<List<Object>> resTmp = new ArrayList<>();
                List<Object> tmp = new ArrayList<>(selectMethod.getAGGREGAT().size());
                boolean havingBool = true;
                for (int idxOfAggregat = 0; idxOfAggregat < selectMethod.getAGGREGAT().size(); idxOfAggregat++) {
                    Aggregat aggregat = selectMethod.getAGGREGAT().get(idxOfAggregat);
                    if (aggregat.getTypeAggregat().equals("COUNT")) {
                        aggregat.setTypeAggregat("SUM");
                        tmp.add(aggregat.applyAggregat(res, idxOfAggregat, TypeDB.INT));
                        aggregat.setTypeAggregat("COUNT");
                        if (aggregat.getHAVING() != null && !aggregat.getHAVING().checkCondition(tmp, idxOfAggregat, TypeDB.INT)) {
                            havingBool = false;
                            break;
                        }
                    } else {
                        tmp.add(aggregat.applyAggregat(res, idxOfAggregat, table.getColumnByName(aggregat.getNameColumn()).getType()));
                        if (aggregat.getHAVING() != null && !aggregat.getHAVING().checkCondition(tmp, idxOfAggregat, table.getColumnByName(aggregat.getNameColumn()).getType())) {
                            havingBool = false;
                            break;
                        }
                    }
                }
                if (havingBool)
                    resTmp.add(tmp);
                res = resTmp;
            }
        }

        System.out.println(res.size());
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

        //ExecutorService executorService = Executors.newFixedThreadPool(2);
        //executorService.submit( ()-> Forwarder.forwardRowsToTable(addressIp1,tableName,listArgs.subList(0, listArgs.size()/3)) );
        //executorService.submit( ()-> Forwarder.forwardRowsToTable(addressIp2,tableName,listArgs.subList(listArgs.size()/3, 2*listArgs.size()/3)) );
        table.addAllRows(listArgs.subList(2*listArgs.size()/3, listArgs.size()).parallelStream().map( list -> Utils.castRow(list, table.getColumns())).collect(Collectors.toList()));
        //executorService.shutdown();
        return "Rows added successfully !";
    }
}