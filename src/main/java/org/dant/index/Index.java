package org.dant.index;

import gnu.trove.TIntArrayList;

import java.util.Set;

public interface Index {

    TIntArrayList getIndexFromValue(Object object);

    void addIndex(Object object, int index);

    Set<Object> getKeys();

}
