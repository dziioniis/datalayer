package com.lambda.collect.datalayer.builder;

import com.lambda.collect.datalayer.db.DbSession;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Transaction {
    private final DbSession dbSession;
    private final List<String> savePoints = new ArrayList<>();

    public Transaction(DbSession dbSession) {
        this.dbSession = dbSession;
    }

    public Transaction begin() throws SQLException {
        this.dbSession.execute("BEGIN;");
        return this;
    }

    public Transaction commit() throws SQLException {
        this.dbSession.execute("COMMIT;");
        return this;
    }

    public Transaction savePoint(String name) throws SQLException {
        this.dbSession.execute("SAVEPOINT \"" + name + "\";");
        savePoints.add(name);
        return this;
    }

    public Transaction rollback(String savePoint) throws SQLException {
        if (savePoint.isEmpty()) {
            this.dbSession.execute("ROLLBACK;");
        } else {
            this.dbSession.execute("ROLLBACK TO \"" + savePoints.get(savePoints.indexOf(savePoint)) + "\";");
        }
        return this;
    }
}
