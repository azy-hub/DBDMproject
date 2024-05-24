package org.dant.index;

public class IndexFactory {

    public enum Type{
        HASH, RED_BLACK_TREE;
    }

    public static Index create(Type type) {
        switch (type) {
            case HASH :
                return new HashIndex();
            case RED_BLACK_TREE:
                return new RedBlackTreeIndex();
            default:
                throw new UnsupportedOperationException();
        }
    }
    public static Index create() {
        return create(Type.HASH);
    }

    private IndexFactory() {
    }
}
