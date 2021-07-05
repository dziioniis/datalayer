package com.lambda.collect.datalayer.db;

import com.lambda.collect.datalayer.tables.AbstractTable;

import java.sql.Connection;
import java.sql.SQLException;

public class Db {
    private final Connection connection;

    public Db(Connection connection) {
        this.connection = connection;
    }

    public <T extends AbstractTable<?>> T use(Class<T> table) throws Exception {
        T newInstance = table.newInstance();
        newInstance.withConnection(connection);
        return newInstance;
    }

    public <T extends AbstractTable<?>> T use(T table) {
        table.withConnection(connection);
        return table;
    }

    public void closeConnection() throws SQLException {
        connection.close();
    }

    public DbSession getDbSession(){
        return new DbSession(this.connection);
    }
}
