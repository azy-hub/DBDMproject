package org.dant.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.dant.model.Column;
import org.dant.select.SelectMethod;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

public class Forwarder {

    static HttpClient httpClient = HttpClient.newHttpClient();

    static ObjectMapper objectMapper = new ObjectMapper();

    public static void forwardRowsToTable(String ipAddress, String name, List<List<Object>> rows) {
        String url = "http://" + ipAddress + ":" + 8080 + "/slave/insertRows/" + name;
        System.out.println("debut d'envoie");
        String jsonBody;
        try {
            jsonBody = objectMapper.writeValueAsString(rows);
        } catch (JsonProcessingException e) {
            System.out.println("Erreur lors de la sérialisation des données en JSON");
            return;
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        System.out.println("Envoie à :" + ipAddress+ "avec une taille de : "+jsonBody.length());
        try {
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            System.out.println("Erreur in forwarding row to other slave node");
        }
    }

    public static void forwardCreateTable(String ipAddress, String name, List<Column> columns) {
        String url = new StringBuilder().append("http://").append(ipAddress).append(":").append(8080).append("/slave/createTable/").append(name).toString();
        String jsonBody;
        try {
            jsonBody = objectMapper.writeValueAsString(columns);
        } catch (JsonProcessingException e) {
            System.out.println("Erreur lors de la sérialisation des données en JSON");
            return;
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.out.println("url : "+url);
            System.out.println("Erreur lors de l'envoie de la diffusion de la requête create table. " + e.getMessage());
        }
    }

    public static List<List<Object>> forwardSelect(String ipAddress, SelectMethod selectMethod) {
        String url = "http://" + ipAddress + ":" + 8080 + "/slave/select";
        // Convertissez la liste en JSON à l'aide de Jackson
        Gson gson = new Gson();
        String jsonBody = gson.toJson(selectMethod);
        System.out.println(jsonBody);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return objectMapper.readValue(response.body(), new TypeReference<List<List<Object>>>() {});

        } catch (Exception e) {
            System.out.println("Erreur lors de l'envoie de la diffusion de la requête select. " + e.getMessage());
        }
        return null;
    }

    public static void forwardColumnsToIndex(String ipAddress, String name, List<String> columnsName) {
        String url = new StringBuilder().append("http://").append(ipAddress).append(":").append(8080).append("/slave/indexTable/").append(name).toString();
        String jsonBody;
        try {
            jsonBody = objectMapper.writeValueAsString(columnsName);
        } catch (JsonProcessingException e) {
            System.out.println("Erreur lors de la sérialisation des données en JSON");
            return;
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        try {
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.out.println("url : "+url);
            System.out.println("Erreur lors de l'envoie de la diffusion de la requête pour indexer les colonnes. " + e.getMessage());
        }
    }

}
