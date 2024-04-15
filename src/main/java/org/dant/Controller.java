package org.dant;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import io.quarkus.vertx.http.Compressed;
import jakarta.ws.rs.*;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.dant.model.*;
import org.jboss.resteasy.reactive.RestPath;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.GZIPOutputStream;


@Path("/v1")
public class Controller {

    @POST
    @Path("/createTableFromParquet/{tableName}")
    @Produces(MediaType.TEXT_PLAIN)
    public void createTableFromParquet(@RestPath String tableName) throws IOException {
        org.apache.hadoop.fs.Path parquetFilePath = new org.apache.hadoop.fs.Path("./src/main/resources/yellow_tripdata_2009-02.parquet");
        Configuration configuration = new Configuration();
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), parquetFilePath)
                .withConf(configuration)
                .build()) {
            Group record;
            MessageType schema = (MessageType) reader.read().getType();
            List<Column> columns = new ArrayList<>();
            for(ColumnDescriptor columnDescriptor : schema.getColumns()) {
                columns.add(new Column(columnDescriptor.getPrimitiveType().getName(),columnDescriptor.getType().toString()));
            }
            createTable(tableName,columns);
        } catch (IOException e) {
            System.out.println("IO Exception : "+e.getMessage());
        }
    }

    @POST
    @Path("/addRowFromParquet/{tableName}")
    @Produces(MediaType.TEXT_PLAIN)
    public void addRowFromParquet(@RestPath String tableName) throws IOException {
        org.apache.hadoop.fs.Path parquetFilePath = new org.apache.hadoop.fs.Path("./src/main/resources/yellow_tripdata_2009-02.parquet");
        Configuration configuration = new Configuration();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try (ParquetReader<Group> reader1 = ParquetReader.builder(new GroupReadSupport(), parquetFilePath)
                .withConf(configuration)
                .build();
        ParquetReader<Group> reader2 = ParquetReader.builder(new GroupReadSupport(), parquetFilePath)
                .withConf(configuration)
                .build()) {
            Group record1;
            Table table = DataBase.get().get(tableName);
            List<Column> columns = table.getColumns();
            /*Future<Void> future = executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    long desiredRecord = 5000000;
                    Group record2;
                    try {
                        for (long i = 0; i < desiredRecord; i++) {
                            record2 = reader2.read();
                        }
                        int cpt = 0;
                        while (cpt < 10000) {
                            record2 = reader2.read();
                            List<List<Object>> listOfList = new ArrayList<>();
                            List<Object> list = new ArrayList<>();
                            listOfList.clear();
                            for (int i = 0; i < 500; i++) {
                                list.clear();
                                for (Column column : columns) {
                                    try {
                                        switch (column.getType()) {
                                            case "BINARY":
                                                list.add(record2.getBinary(column.getName(), 0).toStringUsingUTF8());
                                                break;
                                            case "INT64":
                                                list.add(record2.getLong(column.getName(), 0));
                                                break;
                                            case "DOUBLE":
                                                list.add(record2.getDouble(column.getName(), 0));
                                                break;
                                            default:
                                                list.add(null);
                                                break;
                                        }
                                    } catch(RuntimeException e) {
                                        list.add(null);
                                    }
                                }
                                listOfList.add(list);
                            }
                            forwardRowToTable("172.20.10.2", tableName, listOfList);
                            cpt++;
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return (Void)null;
                }
            });*/
            int cpt = 0;
            while (cpt < 1000) {
                record1 = reader1.read();
                List<Object> list = new ArrayList<>();
                for (Column column : columns) {
                    try {
                        switch (column.getType()) {
                            case "BINARY":
                                list.add(record1.getBinary(column.getName(), 0).toStringUsingUTF8());
                                break;
                            case "INT64":
                                list.add(record1.getLong(column.getName(), 0));
                                break;
                            case "DOUBLE":
                                list.add(record1.getDouble(column.getName(), 0));
                                break;
                            default:
                                list.add(null);
                                break;
                        }
                    } catch (RuntimeException e) {
                        list.add(null);
                    }
                }
                table.addRow(list);
                cpt++;
            }
            //future.get();
            System.out.println("FINI !");
        } catch (IOException e) {
            System.out.println("IO Exception : "+e.getMessage());
        } /*catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }*/
    }

    private void forwardRowToTable(String ipAddress, String name, List<List<Object>> row) {
        String serverAddress = ipAddress;
        int serverPort = 8080;
        String endpoint = "/slave/addRowToTable/"+name;
        String url = "http://" + serverAddress + ":" + serverPort + endpoint;
        HttpClient httpClient = HttpClient.newHttpClient();
        Gson gson = new Gson();
        String jsonBody = gson.toJson(row);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            CompletableFuture<HttpResponse<Void>> futureResponse = httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @POST
    @Path("/DataBase")
    public void getAllTables() {
        System.out.println("Les Tables sont : ");
        Map<String, Table> map = DataBase.get();
        for(String name : map.keySet()) {
            System.out.println(name);
            List<Column> columns = map.get(name).getColumns();
            for( Column c : columns ) {
                System.out.println("Colonne : "+ c.getName() + ", Type : "+c.getType());
            }
        }
    }

    @POST
    @Path("/createTable/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createTable(@RestPath String name, List<Column> listColumns) {
        System.out.println("Create table "+name);
        if( listColumns.isEmpty() )
            new Table(name);
        else
            new Table(name, listColumns);
        //forwardCreateTable("172.20.10.2",name,listColumns);
        //forwardCreateTable("172.20.10.4",name,listColumns);
        System.out.println("Table created successfully");
    }

    private void forwardCreateTable(String ipAddress, String name, List<Column> columns) {
        String url = "http://" + ipAddress + ":" + 8080 + "/slave/createTable/"+name;
        HttpClient httpClient = HttpClient.newHttpClient();
        Gson gson = new Gson();
        String jsonBody = gson.toJson(columns);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            /*// Vérification du code de statut
            int statusCode = response.statusCode();
            if (statusCode == 200) {
                // Lecture du corps de la réponse
                String responseBody = response.body();
                System.out.println("Réponse du serveur : " + responseBody);
            } else {
                System.out.println("Erreur lors de la requête : " + statusCode);
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GET
    @Path("/select/{tableName}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<List<Object>> getTableContent(@RestPath("tableName") String tableName, Condition condition) {
        Table table = DataBase.get().get(tableName);
        if (table == null)
            throw new NotFoundException("La table avec le nom " + tableName + " n'a pas été trouvée.");
        if (condition == null)
            return table.getRows();
        List<List<Object>> res = new LinkedList<>();
        if (!table.checkCondition(condition))
            return res;
        int idx = table.getIndexOfColumnByCondition(condition);
        String type = table.getColumns().get(idx).getType();
        for( List<Object> tmp : table.getRows() ) {
            if (condition.checkCondition(tmp,idx,type))
                res.add(tmp);
        }
        System.out.println(res.size());
        return res;
    }

    private List<List<Object>> forwardGetTableContent(String ipAddress, String name) {
        int serverPort = 8080;
        String endpoint = "/slave/select/"+name;
        String url = "http://" + ipAddress + ":" + serverPort + endpoint;
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "text/plain")
                .GET()
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println(response.body());
            Gson gson = new Gson();
            Type listType = new TypeToken<List<List<Object>>>(){}.getType();
            return gson.fromJson(response.body(), listType);

        } catch (Exception e) {
            e.printStackTrace();
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