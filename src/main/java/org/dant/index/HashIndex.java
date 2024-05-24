package org.dant.index;

import gnu.trove.TIntArrayList;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HashIndex implements Index {

    private Map<Object, TIntArrayList> index;

    public HashIndex() {
        this.index = createMap();
    }
    protected Map<Object,TIntArrayList> createMap() { return new HashMap<>(); }
    public Map<Object, TIntArrayList> getIndex() {
        return index;
    }

    public void setIndex(Map<Object, TIntArrayList> index) {
        this.index = index;
    }

    @Override
    public TIntArrayList getIndexFromValue(Object object) {
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
