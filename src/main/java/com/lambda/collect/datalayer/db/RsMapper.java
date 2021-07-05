package com.lambda.collect.datalayer.db;

import com.lambda.collect.datalayer.tables.AbstractTable;
import com.lambda.collect.datalayer.types.*;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public class RsMapper {
    public final Map<String, Object> cache;

    public RsMapper(Map<String, Object> cache) {
        this.cache = cache;
    }

    public Integer getInt(Column<PgInteger> column) throws SQLException {
        return (Integer) cache.get(column.getAlias());
    }

    public BigDecimal getBigDecimal(Column<PgBigDecimal> column) {
        return (BigDecimal) cache.get(column.getAlias());
    }

    public String getText(Column<PgText> column) {
        return (String) cache.get(column.getAlias());
    }

    public Boolean getBoolean(Column<PgBoolean> column) {
        return (Boolean) cache.get(column.getAlias());
    }

    public Date getTimeStamp(Column<PgTimestamp> column) {
        return (Date) cache.get(column.getAlias());
    }

    public Timestamp getTimeStampz(Column<PgTimestamp> column) {
        return (Timestamp) cache.get(column.getAlias());
    }

    public Time getTime(Column<PgTime> column) {
        return (Time) cache.get(column.getAlias());
    }

    public String getVarchar(Column<PgVarchar> column) {
        return (String) cache.get(column.getAlias());
    }

    public String getJsonb(Column<PgJsonb> column) {
        try {
            return cache.get(column.getAlias()).toString();
        } catch (NullPointerException e) {
            return null;
        }
    }

    public boolean getResultFrom(AbstractTable<?> table) {
        for (Map.Entry<String, Object> row : cache.entrySet()) {
            if (row.getKey().contains(table.name().replace(".", "_"))) {
                return true;
            }
        }
        return false;
    }
}
