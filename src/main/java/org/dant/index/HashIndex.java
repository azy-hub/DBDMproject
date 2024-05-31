package org.dant.index;

import gnu.trove.TIntArrayList;
import org.dant.commons.SpinLock;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HashIndex implements Index {

    private Map<Object, TIntArrayList> index;

    private Object lastObjectAdded = null;
    private TIntArrayList lastIndexList = null;
    private SpinLock lock = new SpinLock();

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
    public void addIndex(Object object, int indexValue) {
        if(object.equals(lastObjectAdded)) {
            lock.lock();
            lastIndexList.add(indexValue);
            lock.unlock();
        } else {
            lock.lock();
            lastIndexList = this.index.computeIfAbsent(object, k -> new TIntArrayList());
            lastIndexList.add(indexValue);
            lock.unlock();
            lastObjectAdded = object;
        }
    }

    @Override
    public Set<Object> getKeys() {
        return this.index.keySet();
    }
}
