package org.dant.model;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Rows<T> extends AbstractCollection<T> implements Collection<T>, Iterable<T> {

    private Chainon <T> first = null ;
    private Chainon <T> last = null ;
    private int size = 0;

    public Chainon<T> getFirst() {
        return first;
    }

    public void setFirst(Chainon<T> first) {
        this.first = first;
    }

    public Chainon<T> getLast() {
        return last;
    }

    public void setLast(Chainon<T> last) {
        this.last = last;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean add(T element ) {
        if ( first == null ) {
            first = new Chainon <T >( element , first );
            last = first ;
        } else {
            last . setNext ( new Chainon <T >( element , null )) ;
            last = last . getNext () ;
        }
        size ++;
        return true ;
    }

    public int size() { return size ; }

    public T get(int index) {
        if ( index < 0 || index >= size () )
            return null ;
        Chainon <T > cur = first ;
        for (int i = 0; i < index ; i ++) {
            cur = cur . getNext () ;
        }
        return cur . getData () ;
    }

    @Override
    public void clear() {
        first = null;
        last = null;
        size = 0;
    }

    public void addAll(Rows<T> rows) {
        this.last.setNext(rows.getFirst());
    }

    @Override
    public Iterator<T> iterator() {
        return new LinkedListIterator();
    }

    public class LinkedListIterator implements Iterator<T> {
        Chainon<T> currentElt = first;

        @Override
        public boolean hasNext() {
            return currentElt.getNext() != null;
        }

        @Override
        public T next() {
            T elt = currentElt.getData();
            currentElt = currentElt.getNext();
            return elt;
        }

    }


    public class Chainon<T> {
        private T data ;
        private Chainon<T> next ;
        public Chainon(T data, Chainon<T> next )
        {
            this.data = data ;
            this.next = next ;
        }
        public T getData() {
            return data ;
        }
        public Chainon<T> getNext() {
            return next ;
        }
        public void setNext(Chainon<T> next) {
            this.next = next ;
        }
    }
}
