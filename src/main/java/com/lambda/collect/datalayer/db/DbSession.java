package com.lambda.collect.datalayer.db;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbSession {
    private Statement statement;
    private ResultSet rs;
    private final Connection connection;
    private final List<Map<String, Object>> cache = new ArrayList<>();

    public DbSession(Connection connection) {
        this.connection = connection;
    }

    public void select(String query) throws SQLException {
        statement = connection.createStatement();
        rs = statement.executeQuery(query);
        while (rs.next()) {
            cache();
        }
        rs.close();
    }

    public void execute(String query) throws SQLException {
        statement = connection.createStatement();
        System.out.println(query);
        statement.executeUpdate(query);
    }

    public void update(String query) throws SQLException {
        statement = connection.createStatement();
        statement.executeUpdate(query);
    }

    public void insert(String query) throws SQLException {
        statement = connection.createStatement();
        rs = statement.executeQuery(query);
        while (rs.next()) {
            cache();
        }
        rs.close();
    }

    public <K> List<K> mapSelect(RowMapper<K> mapper) throws SQLException, JsonProcessingException {
        List<K> response = new ArrayList<>();
        for (Map<String, Object> row : cache) {
            K mapped = mapper.map(new RsMapper(row));
            response.add(mapped);
        }
        return response;
    }

    public List<Map<String, Object>> getMap() {
        return cache;
    }

    public void close() throws SQLException {
        if (rs != null) {
            rs.close();
        }
        statement.close();
    }

    public void cache() throws SQLException {
        HashMap<String, Object> map = new HashMap<>();

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        for (int index = 1; index <= columnCount; index++) {
            String column = rsmd.getColumnName(index);
            Object value = rs.getObject(column);
            map.put(column, value);
        }

        cache.add(map);
    }
}
