package com.lambda.collect.datalayer.db;

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DbConnector {
    private String host = "";
    private String db = "";
    private final Map<String, String> queryParams = new HashMap<>();

    private DbConnector(String driver) throws ClassNotFoundException {
        Class.forName(driver);
    }

    public static DbConnector initPostgresDriver() throws ClassNotFoundException {
        // check if exists, check if connected
        return new DbConnector("org.postgresql.Driver");
    }

    public DbConnector localhost() {
        this.host = "localhost";
        return this;
    }

    public DbConnector host(String host) {
        this.host = host;
        return this;
    }

    public DbConnector dbName(String db) {
        this.db = db;
        return this;
    }

    public DbConnector user(String user) {
        this.queryParams.put("user", user);
        return this;
    }

    public DbConnector password(String password) {
        this.queryParams.put("password", password);
        return this;
    }

    public Db connect() throws Exception {
        if (host.isEmpty()) {
            // custom exception
            throw new Exception();
        }

        if (db.isEmpty()) {
            // custom exception
            throw new Exception();
        }

        StringBuilder mainUrl = new StringBuilder("jdbc:postgresql://{{localhost}}/{{db}}"
                .replace("{{localhost}}", this.host)
                .replace("{{db}}", this.db));

        if (queryParams.entrySet().size() == 2) {
            String user = "";
            String password = "";
            Set<Map.Entry<String, String>> entries = queryParams.entrySet();

            for (Map.Entry<String, String> entry : entries) {
                if (entry.getKey().equals("password")) {
                    password = entry.getValue();
                }
                user = entry.getValue();
            }

            return new Db(DriverManager.getConnection(mainUrl.toString(), user, password));
        }

        for (int i = 0; i < queryParams.entrySet().size(); i++) {
            Set<Map.Entry<String, String>> entries = queryParams.entrySet();

            for (Map.Entry<String, String> entry : entries) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (i == 0) {
                    mainUrl.append("?");
                } else {
                    mainUrl.append("&");
                }
                mainUrl.append("{{key}}={{value}}".replace("{{key}}", key).replace("{{value}}", value));
            }
        }

        return new Db(DriverManager.getConnection(mainUrl.toString()));
    }
}
