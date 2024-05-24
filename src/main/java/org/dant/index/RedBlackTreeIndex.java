package org.dant.index;

import gnu.trove.TIntArrayList;

import java.util.Map;
import java.util.TreeMap;

public class RedBlackTreeIndex extends HashIndex {


    @Override
    protected Map<Object, TIntArrayList> createMap() {
        return new TreeMap<>();
    }
}
