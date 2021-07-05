package com.lambda.collect.datalayer.db;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.sql.SQLException;

public interface RowMapper<K> {
    K map(RsMapper rs) throws SQLException, JsonProcessingException;
}
