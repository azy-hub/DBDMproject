package org.dant.compressor;

import org.dant.model.Column;

import java.util.List;

public class NoCompressor implements Compressor {
    @Override
    public List<List<Object>> compress(List<List<Object>> rows, List<Column> columns) {
        return rows;
    }

    @Override
    public List<List<Object>> uncompress(List<List<Object>> rows, List<Column> columns) {
        return rows;
    }
}
