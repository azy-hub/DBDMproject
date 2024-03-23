package org.dant;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;


import jakarta.ws.rs.core.MediaType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;


import java.io.IOException;


@Path("/hello")
public class GreetingResource {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() throws IOException {

        org.apache.hadoop.fs.Path parquetFilePath = new org.apache.hadoop.fs.Path("src/main/resources/yellow_tripdata_2009-01.parquet");

        // Configuration Hadoop
        Configuration configuration = new Configuration();

        // Définir le schéma
        MessageType schema = MessageTypeParser.parseMessageType(
                "message example {\n" +
                        "  required int32 id;\n" +
                        "  required binary name;\n" +
                        "}");

        // Lire les données Parquet
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), parquetFilePath)
                .withConf(configuration)
                .build()) {
            Group record;
            long cpt = 0;
            while ((record = reader.read()) != null) {
                cpt++;
            }
            System.out.println("CPT : "+cpt);
        } catch (IOException e) {
            System.out.println("IO Exception : "+e.getMessage());
        }

        return "Hello from RESTEasy Reactive";
    }

}
