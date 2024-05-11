package org.dant;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jakarta.ws.rs.core.MediaType;
import org.dant.model.Column;
import org.dant.select.SelectMethod;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

public class Forwarder {

    static HttpClient httpClient = HttpClient.newHttpClient();

    static Gson gson = new Gson();


    public static String forwardFileToTable(String ipAddress, String name, File file, int pos) throws FileNotFoundException {
        String url = "http://" + ipAddress + ":" + 8080 + "/slave/parse/" + name + "/" + pos;

        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.ofFile(file.toPath());
        System.out.println(body.contentLength());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
                .POST(body)
                .build();
        try {
            System.out.println("sending request");
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Liza : " + response.body());
            return response.body();
        } catch (Exception e) {
            System.out.println("Erreur in forwarding row to other slave node");
        }
        return "failed";
    }

    public static void forwardRowsToTable(String ipAddress, String name, List<List<Object>> rows) {
        String url = "http://" + ipAddress + ":" + 8080 + "/slave/insertRows/" + name;
        String jsonBody = gson.toJson(rows);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            System.out.println("Sending Request to Liza");
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());
            System.out.println("request sent !");
        } catch (Exception e) {
            System.out.println("Erreur in forwarding row to other slave node");
        }
    }

    public static void forwardRowToTable(String ipAddress, String name, List<Object> row) {
        String url = new StringBuilder().append("http://").append(ipAddress).append(":").append(8080).append("/slave/insertOneRow/").append(name).toString();
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

    public static void forwardCreateTable(String ipAddress, String name, List<Column> columns) {
        String url = new StringBuilder().append("http://").append(ipAddress).append(":").append(8080).append("/slave/createTable/").append(name).toString();
        String jsonBody = gson.toJson(columns);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.out.println("url : "+url);
            System.out.println("Erreur lors de l'envoie de la diffusion de la requête create table. " + e.getMessage());
        }
    }

    public static List<List<Object>> forwardGetTableContent(String ipAddress, SelectMethod selectMethod) {
        String url = "http://" + ipAddress + ":" + 8080 + "/slave/select";
        String jsonBody = gson.toJson(selectMethod);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
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

}
