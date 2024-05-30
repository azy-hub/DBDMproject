package org.dant.compressor;

import org.dant.model.Column;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;


public class ByteCompressor implements Compressor {
    @Override
    public List<List<Object>> compress(List<List<Object>> rows, List<Column> columns) {
        List<List<Object>> response = new ArrayList<>(rows.size());
        for(List<Object> row : rows) {
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                 DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream)) {

                    // Écrire la taille de la liste pour faciliter la décompression
                objectOutputStream.writeInt(row.size());

                    // Sérialiser et compresser chaque objet de la liste
                for (Object obj : row) {
                    objectOutputStream.writeObject(obj);
                }

                objectOutputStream.flush();
                deflaterOutputStream.finish(); // Assurer que toutes les données sont écrites
                response.add( Collections.singletonList( byteArrayOutputStream.toByteArray()) );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return response;
    }

    @Override
    public List<List<Object>> uncompress(List<List<Object>> rows, List<Column> columns) {
        return null;
    }

    public static byte[] compressList(List<Object> list) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
             DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream)) {

            // Écrire la taille de la liste pour faciliter la décompression
            objectOutputStream.writeInt(list.size());

            // Sérialiser et compresser chaque objet de la liste
            for (Object obj : list) {
                objectOutputStream.writeObject(obj);
            }

            objectOutputStream.flush();
            deflaterOutputStream.finish(); // Assurer que toutes les données sont écrites
        }
        return byteArrayOutputStream.toByteArray();
    }
    public static List<Object> decompressList(byte[] compressedData, int size) throws IOException, ClassNotFoundException {
        List<Object> decompressedList = new ArrayList<>();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedData);
        try (InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
             ObjectInputStream objectInputStream = new ObjectInputStream(inflaterInputStream)) {

            // Lire la taille de la liste
            // Décompresser et désérialiser chaque objet
            for (int i = 0; i < size; i++) {
                Object obj = objectInputStream.readObject();
                decompressedList.add(obj);
            }
        }
        return decompressedList;
    }
}
