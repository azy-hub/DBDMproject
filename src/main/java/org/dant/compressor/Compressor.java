package org.dant.compressor;

import org.dant.model.Column;

import java.io.IOException;
import java.util.List;

public interface Compressor {
    List<List<Object>> compress(List<List<Object>> rows);
    List<List<Object>> uncompress(List<List<Object>> rows);

    List<Object> compressList(List<Object> row);
    List<Object> uncompressList(List<Object> row);

}
