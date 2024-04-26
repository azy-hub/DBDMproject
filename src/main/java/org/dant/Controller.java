package org.dant;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jakarta.annotation.Nullable;
import jakarta.ws.rs.*;

import jakarta.ws.rs.core.MediaType;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.RecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.schema.MessageType;
import org.dant.model.*;
import org.jboss.resteasy.reactive.RestPath;



import java.io.IOException;
import java.io.ByteArrayInputStream;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


@Path("/v1")
public class Controller {

    private final ExecutorService executorService = Executors.newCachedThreadPool();
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
    public void readParquet(@RestPath String tableName, InputStream inputStream) throws IOException {
        byte[] data = inputStream.readAllBytes();
            try {
                Forwarder.forwardParquet(addressIp1,tableName,new ByteArrayInputStream(data),0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        //executorService.submit( () -> Forwarder.forwardParquet(addressIp2,tableName,inputStream,1) );
        Configuration conf = new Configuration();
        Table table = DataBase.get().get(tableName);
        if (table == null)
            return;
        FileSystem fs = null;
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("temp.parquet");
        try {
            fs = FileSystem.get(conf);
            System.out.println("deb copiee");
            try (FSDataOutputStream outputStream = fs.create(path)) {
                IOUtils.copy(new ByteArrayInputStream(data), outputStream);
            }
            System.out.println("end copiee");
        } catch (IOException e) {
            System.out.println("Erreur en recopiant le fichier parquet lu");
        };
        try (ParquetFileReader parquetFileReader = new ParquetFileReader(HadoopInputFile.fromPath(path, new Configuration()), ParquetReadOptions.builder().build())) {
            ParquetMetadata footer = parquetFileReader.getFooter();
            MessageType schema = footer.getFileMetaData().getSchema();
            PageReadStore pages;

            while ((pages = parquetFileReader.readNextRowGroup()) != null) {
                System.out.println("try");
                long rows = 300000;//pages.getRowCount();
                RecordReader<Group> recordReader = new ColumnIOFactory().getColumnIO(schema).getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i=0; i<2*(rows/3); i++) {
                    recordReader.read();
                }
                ExecutorService executorService = Executors.newFixedThreadPool(3);
                for(long row=2*(rows/3); row<rows; row++) {
                    SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                    executorService.submit( ()-> table.addRowFromSimpleGroup(simpleGroup));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (fs != null)
                    fs.delete(path, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
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
        Forwarder.forwardCreateTable(addressIp1,tableName,listColumns);
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

        if (!table.checkConditions(selectMethod.getWHERE()))
            throw new NotFoundException("No condition found");


        if (table.getColumnsByNames(selectMethod.getSELECT()).isEmpty())
            throw new NotFoundException("No columns matching with any of this names " + selectMethod.getSELECT());

        Future<List<List<Object>>> future1 = executorService.submit(new Callable<List<List<Object>>>() {
            @Override
            public List<List<Object>> call() throws Exception {
                return Forwarder.forwardGetTableContent(addressIp1,selectMethod);
            }
        });
        /*Future<List<List<Object>>> future2 = executorService.submit(new Callable<List<List<Object>>>() {
            @Override
            public List<List<Object>> call() throws Exception {
                return Forwarder.forwardGetTableContent(addressIp2,selectMethod);
            }
        });*/

        List<List<Object>> res = table.select(selectMethod);
        try {
            res.addAll(future1.get());
            //res.addAll(future2.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
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
        if( !listArgs.stream().allMatch( list -> list.size() == table.getColumns().size() ) )
            throw new NotFoundException("Nombre d'arguments invalide dans l'une des lignes.");
        executorService.submit( ()-> Forwarder.forwardRowsToTable(addressIp1,tableName,listArgs.subList(0, listArgs.size()/3)) );
        executorService.submit( ()-> Forwarder.forwardRowsToTable(addressIp2,tableName,listArgs.subList(listArgs.size()/3, 2*listArgs.size()/3)) );
        table.addAllRows(listArgs.subList(2*listArgs.size()/3, listArgs.size()).parallelStream().map( list -> table.castRow(list)).collect(Collectors.toList()));
        return "Rows added successfully !";
    }
}