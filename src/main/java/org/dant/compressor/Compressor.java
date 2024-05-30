package org.dant.compressor;

import org.dant.model.Column;

import java.util.List;

public interface Compressor {
    List<List<Object>> compress(List<List<Object>> rows, List<Column> columns);
    List<List<Object>> uncompress(List<List<Object>> rows, List<Column> columns);
}
