package org.dant.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataBase {

    public static Map<String,Table> get() {
        return Tables.TABLES;
    }

    private static class Tables {

        private static final Map<String,Table> TABLES = new HashMap<>();

    }

}
