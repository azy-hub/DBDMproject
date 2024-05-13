package org.dant.index;

import gnu.trove.TIntArrayList;

import java.util.Map;

public interface Index {

    public TIntArrayList getIndexsFromValue(Object object);

    public void addIndex(Object object, int index);

}
