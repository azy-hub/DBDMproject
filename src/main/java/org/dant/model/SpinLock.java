package org.dant.model;

import java.util.concurrent.atomic.AtomicBoolean;

public class SpinLock {
    public static AtomicBoolean atomicBoolean = new AtomicBoolean(true);

    public void lock() {
        while (!atomicBoolean.compareAndSet(true,false)) {
        }
    }

    public void unlock() {
        atomicBoolean.compareAndSet(false,true);
    }
}
