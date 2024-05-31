package org.dant.compressor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.dant.commons.TypeDB;
import org.dant.model.Column;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SnappyCompressor implements Compressor{

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<List<Object>> compress(List<List<Object>> rows) {
        try {
            return Collections.singletonList(Collections.singletonList(Snappy.compress(objectMapper.writeValueAsBytes(rows))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<List<Object>> uncompress(List<List<Object>> rows) {
        List<List<Object>> tmp = new ArrayList<>(rows.size());
        for(List<Object> row : rows) {
            tmp.add(uncompressList(row));
        }
        return tmp;
    }

    @Override
    public List<Object> compressList(List<Object> row){
        try (ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutStream = new ObjectOutputStream(byteOutStream)) {
            objectOutStream.writeObject(row);
            return Collections.singletonList(Snappy.compress(byteOutStream.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Object> uncompressList(List<Object> row) {
        try {
            return objectMapper.readValue(Snappy.uncompress( (byte[])  row.get(0)), new TypeReference<List<Object>>() {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
