package org.dant;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.dant.model.Column;
import org.dant.model.SelectMethod;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Forwarder {

    public static void forwardRowsToTable(String ipAddress, String name, List<List<Object>> rows) {
        String url = "http://" + ipAddress + ":" + 8080 + "/slave/insertRows/" + name;
        HttpClient httpClient = HttpClient.newHttpClient();
        Gson gson = new Gson();
        String jsonBody = gson.toJson(rows);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            CompletableFuture<HttpResponse<Void>> response = httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            System.out.println("Erreur in forwarding row to other slave node");
        }
    }

    public static void forwardRowToTable(String ipAddress, String name, List<Object> row) {
        String url = new StringBuilder().append("http://").append(ipAddress).append(":").append(8080).append("/slave/insertOneRow/").append(name).toString();
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

    public static void forwardCreateTable(String ipAddress, String name, List<Column> columns) {
        String url = new StringBuilder().append("http://").append(ipAddress).append(":").append(8080).append("/slave/createTable/").append(name).toString();
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
            System.out.println("url : "+url);
            System.out.println("Erreur lors de l'envoie de la diffusion de la requête create table. " + e.getMessage());
        }
    }

    public static List<List<Object>> forwardGetTableContent(String ipAddress, SelectMethod selectMethod) {
        String url = "http://" + ipAddress + ":" + 8080 + "/slave/select";
        Gson gson = new Gson();
        String jsonBody = gson.toJson(selectMethod);
        HttpClient httpClient = HttpClient.newHttpClient();
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
