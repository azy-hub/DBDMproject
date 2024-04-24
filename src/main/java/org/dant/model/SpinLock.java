package org.dant.model;

import java.util.concurrent.atomic.AtomicBoolean;

public class SpinLock {
    private final AtomicBoolean lock = new AtomicBoolean(true);

    public void lock() {
        while (!lock.compareAndSet(true,false)) {
        }
    }

    public void unlock() {
        lock.compareAndSet(false,true);
    }
}
