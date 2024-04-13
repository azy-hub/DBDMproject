package org.dant;

import com.google.gson.Gson;
import jakarta.ws.rs.*;


import jakarta.ws.rs.core.MediaType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.conf.Configuration;
import org.dant.model.Column;
import org.dant.model.DataBase;
import org.dant.model.Table;
import org.jboss.resteasy.reactive.RestPath;
import java.io.IOException;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Path("/v1")
public class Controller {

    @POST
    @Path("/createTableFromParquet/{tableName}")
    @Produces(MediaType.TEXT_PLAIN)
    public void createTableFromParquet(@RestPath String tableName) throws IOException {
        org.apache.hadoop.fs.Path parquetFilePath = new org.apache.hadoop.fs.Path("./src/main/resources/yellow_tripdata_2009-01.parquet");
        // Configuration Hadoop
        Configuration configuration = new Configuration();
        // Lire les données Parquet
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
            /*Group record;
            long cpt = 0;
            while ((record = reader.read()) != null) {
                cpt++;
            }
            System.out.println("CPT : "+cpt);*/
        } catch (IOException e) {
            System.out.println("IO Exception : "+e.getMessage());
        }
    }

    @POST
    @Path("/addRowFromParquet/{tableName}")
    @Produces(MediaType.TEXT_PLAIN)
    public void addRowFromParquet(@RestPath String tableName) throws IOException {
        org.apache.hadoop.fs.Path parquetFilePath = new org.apache.hadoop.fs.Path("./src/main/resources/yellow_tripdata_2009-01.parquet");
        // Configuration Hadoop
        Configuration configuration = new Configuration();
        // Lire les données Parquet
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), parquetFilePath)
                .withConf(configuration)
                .build()) {
            Group record;
            Table table = DataBase.get().get(tableName);
            List<Column> columns = table.getColumns();
            int cpt = 0;
            while (cpt < 7050000) {
                record = reader.read();
                List<Object> list = new ArrayList<>();
                for (Column column : columns) {
                    try {
                        switch (column.getType()) {
                            case "BINARY":
                                list.add(record.getBinary(column.getName(), 0).toStringUsingUTF8());
                                break;
                            case "INT64":
                                list.add(record.getLong(column.getName(), 0));
                                break;
                            case "DOUBLE":
                                list.add(record.getDouble(column.getName(), 0));
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
            System.out.println("FINI !");
        } catch (IOException e) {
            System.out.println("IO Exception : "+e.getMessage());
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
        forwardCreateTable("172.20.10.3",name,listColumns);
        //forwardCreateTable();
        System.out.println("Table created successfully");
    }

    private void forwardCreateTable(String ipAddress, String name, List<Column> columns) {
        String serverAddress = ipAddress;
        int serverPort = 8080;
        String endpoint = "/slave/createTable/"+name;
        String url = "http://" + serverAddress + ":" + serverPort + endpoint;
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
    public Table getTableContent(@RestPath("tableName") String tableName) {
        Table table = DataBase.get().get(tableName);
        if (table == null)
            throw new NotFoundException("La table avec le nom " + tableName + " n'a pas été trouvée.");
        return table;
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
