package org.dant.index;

import gnu.trove.TIntArrayList;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HashMapIndex implements Index {

    private Map<Object, TIntArrayList> index;

    public HashMapIndex() {
        this.index = new HashMap<>();
    }

    public HashMapIndex(Map<Object, TIntArrayList> index) {
        this.index = index;
    }

    public Map<Object, TIntArrayList> getIndex() {
        return index;
    }

    public void setIndex(Map<Object, TIntArrayList> index) {
        this.index = index;
    }

    @Override
    public TIntArrayList getIndexsFromValue(Object object) {
        return this.index.get(object);
    }

    @Override
    public void addIndex(Object object, int index) {
        this.index.computeIfAbsent(object, k -> new TIntArrayList()).add(index);
    }

    @Override
    public Set<Object> getKeys() {
        return this.index.keySet();
    }

}
