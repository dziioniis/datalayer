package com.lambda.collect.datalayer.tables;

import com.lambda.collect.datalayer.db.RsMapper;
import com.lambda.collect.datalayer.types.Column;
import com.lambda.collect.datalayer.types.PgInteger;
import com.lambda.collect.datalayer.types.PgTimestamp;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MigrationsTable extends AbstractTable<MigrationsTable.Model> {
    public static final MigrationsTable INSTANCE = new MigrationsTable();

    public Column<PgInteger> id = integer("id").autoIncrement();
    public Column<PgInteger> version = integer("version");
    public Column<PgTimestamp> createdAt = timestamp("created_at");

    @Override
    protected Model map(RsMapper rs) throws SQLException {
        return new Model(
                rs.getInt(id),
                rs.getInt(version),
                rs.getTimeStamp(createdAt)
        );
    }

    @Override
    protected Map<String, Object> mapInsert(Model model) throws SQLException {
        HashMap<String, Object> result = new HashMap<>();

        result.put(version.getColumnName(), model.version);
        result.put(createdAt.getColumnName(), new Timestamp(new Date().getTime()));

        return result;
    }

    @Override
    public List<Column<?>> getPrimaryKeys() {
        if (primaryKeys.isEmpty()) {
            primaryKeys.add(id);
        }
        return primaryKeys;
    }

    @Override
    public List<Column<?>> getUniqueKeys() {
        return uniqueKeys;
    }

    @Override
    public String name() {
        return "migrations";
    }

    public static class Model {
        public int id;
        public int version;
        public Date createdAt;

        public Model(int id, int version, Date createdAt) {
            this.id = id;
            this.version = version;
            this.createdAt = createdAt;
        }

        public Model(int version, Date createdAt) {
            this(0, version, createdAt);
        }
    }
}

