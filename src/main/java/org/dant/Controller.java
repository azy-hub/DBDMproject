package org.dant;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jakarta.annotation.Nullable;
import jakarta.ws.rs.*;

import jakarta.ws.rs.core.MediaType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.schema.MessageType;
import org.dant.model.*;
import org.jboss.resteasy.reactive.RestPath;



import java.io.IOException;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


@Path("/v1")
public class Controller {

    @POST
    @Path("/parquet/createTable/{tableName}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public String parseParquet(@RestPath String tableName, InputStream inputStream) {
        Configuration conf = new Configuration();
        FileSystem fs;
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("temp.parquet");
        try {
            fs = FileSystem.get(conf);
            // Copier le contenu de l'inputStream dans un fichier parquet temporaire
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
    public void readParquet(@RestPath String tableName, InputStream inputStream) {
        Configuration conf = new Configuration();
        Table table = DataBase.get().get(tableName);
        if (table == null)
            return;
        FileSystem fs = null;
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("temp.parquet");
        try {
            fs = FileSystem.get(conf);
            try (FSDataOutputStream outputStream = fs.create(path)) {
                IOUtils.copy(inputStream, outputStream);
            }
        } catch (IOException e) {
            System.out.println("Erreur en recopiant le fichier parquet lu");
        };
        try (ParquetFileReader parquetFileReader = new ParquetFileReader(HadoopInputFile.fromPath(path, new Configuration()), ParquetReadOptions.builder().build())) {
            ParquetMetadata footer = parquetFileReader.getFooter();
            MessageType schema = footer.getFileMetaData().getSchema();
            PageReadStore pages;
            while ((pages = parquetFileReader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                RecordReader<Group> recordReader = new ColumnIOFactory().getColumnIO(schema).getRecordReader(pages, new GroupRecordConverter(schema));
                System.out.println(rows);
                for (int row = 0; row < 50; row++) {
                    int fieldIndex = 0;
                    SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                    List<Object> list = new ArrayList<>();
                    for (Column column : table.getColumns()) {
                        try {
                            switch (column.getType()) {
                                case "BINARY":
                                    list.add(simpleGroup.getBinary(fieldIndex, 0).toStringUsingUTF8());
                                    break;
                                case "INT64":
                                    list.add(simpleGroup.getLong(fieldIndex, 0));
                                    break;
                                case "DOUBLE":
                                    list.add(simpleGroup.getDouble(fieldIndex, 0));
                                    break;
                                default:
                                    list.add(null);
                                    break;
                            }
                        } catch (RuntimeException e) {
                            list.add(null);
                        }
                        fieldIndex++;
                    }
                    table.addRow(list);
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

    private void forwardRowToTable(String ipAddress, String name, List<List<Object>> row) {
        String url = "https://" + ipAddress + ":" + 8080 +"/slave/addRowToTable/"+name;
        HttpClient httpClient = HttpClient.newHttpClient();
        Gson gson = new Gson();
        String jsonBody = gson.toJson(row);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            System.out.println("Erreur in forwarding row to other slave node");
        }
    }

    @POST
    @Path("/DataBase")
    public String getAllTables() {
        StringBuilder stringBuilder = new StringBuilder("Les Tables sont : \n");
        Map<String, Table> map = DataBase.get();
        for(String name : map.keySet()) {
            stringBuilder.append("Table ").append(name).append(" :\n");
            List<Column> columns = map.get(name).getColumns();
            for( Column c : columns ) {
                stringBuilder.append("Colonne : ").append(c.getName()).append(", Type : ").append(c.getType());
            }
        }
        return stringBuilder.toString();
    }

    @POST
    @Path("/createTable/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createTable(@RestPath String name,@Nullable List<Column> listColumns) {
        System.out.println("Create table "+name);
        if (DataBase.get().containsKey(name)) {
            System.out.println("Table already exists with name : "+name);
            return;
        }
        //forwardCreateTable("172.20.10.2",name,listColumns);
        //forwardCreateTable("172.20.10.4",name,listColumns);
        if( listColumns.isEmpty() )
            new Table(name);
        else
            new Table(name, listColumns);
        System.out.println("Table created successfully");
    }

    private void forwardCreateTable(String ipAddress, String name, List<Column> columns) {
        String url = "http://" + ipAddress + ":" + 8080 + "/slave/createTable/"+name;
        Gson gson = new Gson();
        String jsonBody = gson.toJson(columns);
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.out.println("Erreur lors de l'envoie de la diffusion de la requête select. " + e.getMessage());
        }
    }

    @POST
    @Path("/select/{tableName}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<List<Object>> getTableContent(@RestPath("tableName") String tableName,@Nullable List<Condition> conditions) {
        Table table = DataBase.get().get(tableName);
        if (table == null)
            throw new NotFoundException("La table avec le nom " + tableName + " n'a pas été trouvée.");
        List<List<Object>> res = new LinkedList<>();
        if (!table.checkConditions(conditions))
            return res;
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<List<List<Object>>> future1 = executorService.submit(new Callable<List<List<Object>>>() {
                @Override
                public List<List<Object>> call() {
                    return forwardGetTableContent("",tableName,conditions);
                }
            });
        Future<List<List<Object>>> future2 = executorService.submit(new Callable<List<List<Object>>>() {
                @Override
                public List<List<Object>> call() {
                    return forwardGetTableContent("",tableName,conditions);
                }
            });
        if (conditions == null || conditions.isEmpty()) {
            res = table.getRows();
        } else {
            List<Integer> idx = table.getIndexOfColumnsByConditions(conditions);
            List<String> type = idx.stream().map(indice -> table.getColumns().get(indice).getType()).toList();
            res = table.getRows().parallelStream().filter(list -> table.validate(list, conditions, idx, type)).collect(Collectors.toList());
        }
        /*try {
            res.addAll(future1.get());
            res.addAll(future2.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }*/
        System.out.println(res.size());
        return res;
    }

    private List<List<Object>> forwardGetTableContent(String ipAddress, String name, List<Condition> conditions) {
        int serverPort = 8080;
        String endpoint = "/slave/select/"+name;
        String url = "http://" + ipAddress + ":" + serverPort + endpoint;
        Gson gson = new Gson();
        String jsonBody = gson.toJson(conditions);
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            Type listType = new TypeToken<List<List<Object>>>(){}.getType();
            return gson.fromJson(response.body(), listType);
        } catch (Exception e) {
            System.out.println("Erreur lors de l'envoie de la diffusion de la requête select. " + e.getMessage());
        }
        return null;
    }

    @POST
    @Path("/addRowToTable/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void addRowToTable(@RestPath String name, List<Object> args) {
        Table table = DataBase.get().get(name);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + name + " n'a pas été trouvée.");
        if(args.size() != table.getColumns().size()) {
            System.out.println("Nombre d'argument incorrect.");
            return;
        }
        table.addRow(args);
    }
}