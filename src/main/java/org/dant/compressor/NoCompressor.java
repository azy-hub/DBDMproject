package org.dant.compressor;

import org.dant.model.Column;

import java.io.IOException;
import java.util.List;

public class NoCompressor implements Compressor {
    @Override
    public List<List<Object>> compress(List<List<Object>> rows) {
        return rows;
    }

    @Override
    public List<List<Object>> uncompress(List<List<Object>> rows) {
        return rows;
    }

    @Override
    public List<Object> compressList(List<Object> row) {
        return row;
    }

    @Override
    public List<Object> uncompressList(List<Object> row) {
        return row;
    }
}
