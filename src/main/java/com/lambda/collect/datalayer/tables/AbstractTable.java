package com.lambda.collect.datalayer.tables;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.lambda.collect.datalayer.builder.*;
import com.lambda.collect.datalayer.builder.types.Filters;
import com.lambda.collect.datalayer.builder.types.Order;
import com.lambda.collect.datalayer.builder.types.Page;
import com.lambda.collect.datalayer.builder.types.Updates;
import com.lambda.collect.datalayer.db.DbSession;
import com.lambda.collect.datalayer.db.RsMapper;
import com.lambda.collect.datalayer.types.*;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public abstract class AbstractTable<K> implements Table {
    private Connection connection;

    protected List<Column<?>> primaryKeys = new ArrayList<>();
    protected List<Column<?>> uniqueKeys = new ArrayList<>();

    public void withConnection(Connection connection) {
        this.connection = connection;
    }

    protected Column<PgVarchar> varchar(String name, int size) {
        return Column.varchar(this, name, size);
    }

    protected Column<PgBigDecimal> decimal(String name, int precision, int scale) {
        return Column.decimal(this, name, precision, scale);
    }

    protected Column<PgText> text(String name) {
        return Column.text(this, name);
    }

    protected Column<PgJsonb> jsonb(String name) {
        return Column.jsonb(this, name);
    }

    protected Column<PgInteger> integer(String name) {
        return Column.integer(this, name);
    }

    protected Column<PgTimestamp> timestamp(String name) {
        return Column.timestamp(this, name);
    }

    protected Column<PgTime> time(String name) {
        return Column.time(this, name);
    }

    protected Column<PgBoolean> bool(String name) {
        return Column.bool(this, name);
    }

    public <T extends ColumnType> List<K> selectWithFiltersExpr(Filters.Expr filters, Order<T> order) throws IllegalAccessException, SQLException, JsonProcessingException {
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).filteredBy(filters).ordered(order).build();
        dbSession.select(query);
        return dbSession.mapSelect(this::map);
    }

    public <T extends ColumnType> List<K> selectWithPage(Filters.Expr filters, Page page) throws IllegalAccessException, SQLException, JsonProcessingException {
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).filteredBy(filters).paged(page).build();
        dbSession.select(query);
        return dbSession.mapSelect(this::map);
    }

    public <T extends ColumnType> List<K> selectExpr(Filters.Expr filters) throws IllegalAccessException, SQLException, JsonProcessingException {
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).filteredBy(filters).build();
        dbSession.select(query);
        return dbSession.mapSelect(this::map);
    }

    public <T extends ColumnType> List<Map<String, Object>> select(String query) throws SQLException, JsonProcessingException {
        DbSession dbSession = new DbSession(connection);
        dbSession.select(query);
        List<Map<String, Object>> map = dbSession.getMap();
        return map;
    }

    public void execute(String query) throws SQLException, JsonProcessingException {
        DbSession dbSession = new DbSession(connection);
        dbSession.execute(query);
    }

    public <T extends ColumnType, T1> SelectRes<K, T1> select(Filters.Expr filters, Join<T, T1> join1, Order<T> order) throws SQLException, JsonProcessingException, IllegalAccessException {
        List<Join<T, ?>> joinPropsList = Collections.singletonList(join1);
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).joined(joinPropsList).filteredBy(filters).ordered(order).build();
        dbSession.select(query);
        List<K> response = dbSession.mapSelect(this::map);
        AbstractTable<T1> parent = join1.joinTable;
        List<T1> objects = dbSession.mapSelect(parent::map);
        dbSession.close();
        return new SelectRes<>(response, objects);
    }

    public <T extends ColumnType, T1> SelectRes<K, T1> select(Filters.Expr filters, Join<T, T1> join1) throws SQLException, JsonProcessingException, IllegalAccessException {
        List<Join<T, ?>> joinPropsList = Collections.singletonList(join1);
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).joined(joinPropsList).filteredBy(filters).build();
        dbSession.select(query);
        List<K> response = dbSession.mapSelect(this::map);
        AbstractTable<T1> parent = join1.joinTable;
        List<T1> objects = dbSession.mapSelect(parent::map);
        dbSession.close();
        return new SelectRes<>(response, objects);
    }

    public static class SelectRes<T1, T2> {
        private final List<T1> t1;
        private final List<T2> t2;

        public SelectRes(List<T1> t1, List<T2> t2) {
            this.t1 = t1;
            this.t2 = t2;
        }

        public <M> List<M> mapRes(IMac<M, T1, T2> imac) {
            List<M> objects = new ArrayList<>();
            for (int i = 0; i < t1.size(); i++) {
                objects.add(imac.mapRes(t1.get(i), t2.get(i)));
            }
            return objects;
        }
    }

    public static class SelectResTwoJoin<T1, T2, T3> {
        private final List<T1> t1;
        private final List<T2> t2;
        private final List<T3> t3;

        public SelectResTwoJoin(List<T1> t1, List<T2> t2, List<T3> t3) {
            this.t1 = t1;
            this.t2 = t2;
            this.t3 = t3;
        }

        public <M> List<M> mapRes(IMacTwo<M, T1, T2, T3> imac) throws JsonProcessingException {
            List<M> objects = new ArrayList<>();
            for (int i = 0; i < t1.size(); i++) {
                objects.add(imac.mapRes(t1.get(i), t2.get(i), t3.get(i)));
            }
            return objects;
        }
    }

    public static class SelectResThreeJoin<T1, T2, T3, T4> {
        private final List<T1> t1;
        private final List<T2> t2;
        private final List<T3> t3;
        private final List<T4> t4;

        public SelectResThreeJoin(List<T1> t1, List<T2> t2, List<T3> t3, List<T4> t4) {
            this.t1 = t1;
            this.t2 = t2;
            this.t3 = t3;
            this.t4 = t4;
        }

        public <M> List<M> mapRes(IMacThree<M, T1, T2, T3, T4> imac) {
            List<M> objects = new ArrayList<>();
            for (int i = 0; i < t1.size(); i++) {
                objects.add(imac.mapRes(t1.get(i), t2.get(i), t3.get(i), t4.get(i)));
            }
            return objects;
        }
    }

    public interface IMac<M, T1, T2> {
        M mapRes(T1 t1, T2 t2);
    }

    public interface IMacTwo<M, T1, T2, T3> {
        M mapRes(T1 t1, T2 t2, T3 t3) throws JsonProcessingException;
    }

    public interface IMacThree<M, T1, T2, T3, T4> {
        M mapRes(T1 t1, T2 t2, T3 t3, T4 t4);
    }

    public <T extends ColumnType, T1, T2> SelectResTwoJoin<K, T1, T2> select(
            Filters.Expr filters,
            Join<T, T1> join1,
            Join<T, T2> join2,
            Order<T> order) throws SQLException, JsonProcessingException, IllegalAccessException {
        List<Join<T, ?>> joinPropsList = Arrays.asList(join1, join2);
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).joined(joinPropsList).filteredBy(filters).ordered(order).build();
        dbSession.select(query);
        List<K> response = dbSession.mapSelect(this::map);
        AbstractTable<T1> parent = join1.joinTable;
        AbstractTable<T2> parentTwo = join2.joinTable;
        List<T1> objects = dbSession.mapSelect(parent::map);
        List<T2> objectsTwo = dbSession.mapSelect(parentTwo::map);
        dbSession.close();
        return new SelectResTwoJoin<>(response, objects, objectsTwo);
    }

    public <T extends ColumnType, T1, T2> SelectResTwoJoin<K, T1, T2> select(
            Filters.Expr filters,
            Join<T, T1> join1,
            Join<T, T2> join2) throws SQLException, JsonProcessingException, IllegalAccessException {
        List<Join<T, ?>> joinPropsList = Arrays.asList(join1, join2);
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).joined(joinPropsList).filteredBy(filters).build();
        dbSession.select(query);
        List<K> response = dbSession.mapSelect(this::map);
        AbstractTable<T1> parent = join1.joinTable;
        AbstractTable<T2> parentTwo = join2.joinTable;
        List<T1> objects = dbSession.mapSelect(parent::map);
        List<T2> objectsTwo = dbSession.mapSelect(parentTwo::map);
        dbSession.close();
        return new SelectResTwoJoin<>(response, objects, objectsTwo);
    }

    public <T extends ColumnType, T1, T2, T3> SelectResThreeJoin<K, T1, T2, T3>
    select(Filters.Expr filters,
           Join<T, T1> join1,
           Join<T, T2> join2,
           Join<T, T3> join3,
           Order<T> order) throws SQLException, JsonProcessingException, IllegalAccessException {
        List<Join<T, ?>> joinPropsList = Arrays.asList(join1, join2, join3);
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).joined(joinPropsList).filteredBy(filters).ordered(order).build();
        dbSession.select(query);
        List<K> response = dbSession.mapSelect(this::map);
        AbstractTable<T1> parent = join1.joinTable;
        AbstractTable<T2> parentTwo = join2.joinTable;
        AbstractTable<T3> parentThree = join3.joinTable;
        List<T1> objects = dbSession.mapSelect(parent::map);
        List<T2> objectsTwo = dbSession.mapSelect(parentTwo::map);
        List<T3> objectsThree = dbSession.mapSelect(parentThree::map);
        dbSession.close();
        return new SelectResThreeJoin<>(response, objects, objectsTwo, objectsThree);
    }

    public <T extends ColumnType, T1, T2, T3> SelectResThreeJoin<K, T1, T2, T3>
    select(Filters.Expr filters,
           Join<T, T1> join1,
           Join<T, T2> join2,
           Join<T, T3> join3) throws SQLException, JsonProcessingException, IllegalAccessException {
        List<Join<T, ?>> joinPropsList = Arrays.asList(join1, join2, join3);
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).joined(joinPropsList).filteredBy(filters).build();
        dbSession.select(query);
        List<K> response = dbSession.mapSelect(this::map);
        AbstractTable<T1> parent = join1.joinTable;
        AbstractTable<T2> parentTwo = join2.joinTable;
        AbstractTable<T3> parentThree = join3.joinTable;
        List<T1> objects = dbSession.mapSelect(parent::map);
        List<T2> objectsTwo = dbSession.mapSelect(parentTwo::map);
        List<T3> objectsThree = dbSession.mapSelect(parentThree::map);
        dbSession.close();
        return new SelectResThreeJoin<>(response, objects, objectsTwo, objectsThree);
    }

    public <T extends ColumnType> K selectFirst(Filters.Expr filters, Order<T> order) throws SQLException, JsonProcessingException, IllegalAccessException {
        return selectWithFiltersExpr(filters, order).get(0);
    }

    public <T extends ColumnType, T1> List<K> select(Filters.Expr filters, Order<T> order) throws SQLException, JsonProcessingException, IllegalAccessException {
        return this.selectWithFiltersExpr(filters, order);
    }

    public <T extends ColumnType, T1> List<K> select(Filters.Expr filters, Page page) throws SQLException, JsonProcessingException, IllegalAccessException {
        return this.selectWithPage(filters, page);
    }

    public List<K> select(Filters.Expr filters) throws SQLException, JsonProcessingException, IllegalAccessException {
        return this.selectExpr(filters);
    }

    public List<K> selectAll() throws SQLException, JsonProcessingException, IllegalAccessException {
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).build();
        dbSession.select(query);
        List<K> response = dbSession.mapSelect(this::map);
        dbSession.close();
        return response;
    }

    public <T extends ColumnType> List<K> selectAll(Order<T> order) throws SQLException, JsonProcessingException, IllegalAccessException {
        DbSession dbSession = new DbSession(connection);
        String query = Select.from(this).build();
        dbSession.select(query);
        List<K> response = dbSession.mapSelect(this::map);
        dbSession.close();
        return response;
    }

    public void update(Updates.UpdateExpr updates) throws Exception {
        this.update(updates, null);
    }

    public void update(Updates.UpdateExpr updates, Filters.Expr filters) throws Exception {
        DbSession dbSession = new DbSession(connection);
        String query = Update.in(this).update(updates).filteredBy(filters).build();
        System.out.println(query);
        dbSession.update(query);
        dbSession.close();
    }

    public void delete(Filters.Expr filters) throws SQLException {
        DbSession dbSession = new DbSession(connection);
        String query = Delete.from(this).filteredBy(filters).build();
        System.out.println(query);
        dbSession.update(query);
        dbSession.close();
    }

    public K insert(K model) throws Exception {
        DbSession dbSession = new DbSession(connection);
        String query = Insert.into(this).values(mapInsert(model)).build();
        log.info("Query: " + query);
        dbSession.insert(query);
        List<K> response = dbSession.mapSelect(this::map);
        dbSession.close();
        return response.get(0);
    }

    public K upsert(K model, Updates.UpdateExpr onConflict) throws Exception {
        DbSession dbSession = new DbSession(connection);
        String query = Insert.into(this).values(mapInsert(model)).onConflict(onConflict).build();
        System.out.println(query);
        dbSession.insert(query);
        List<K> response = dbSession.mapSelect(this::map);
        dbSession.close();
        return response.get(0);
    }

    public List<K> batchInsert(List<K> models) throws Exception {
        DbSession dbSession = new DbSession(connection);
        List<Map<String, Object>> values = new ArrayList<>();
        for (K model : models) {
            values.add(mapInsert(model));
        }
        if (values.isEmpty()) {
            return new ArrayList<>();
        }
        String query = Insert.into(this).values(values).build();
        System.out.println(query);
        dbSession.insert(query);
        List<K> response = dbSession.mapSelect(this::map);
        dbSession.close();
        return response;
    }

    public void migrate() throws SQLException, IllegalAccessException {
        DbSession dbSession = new DbSession(connection);
        String query = Create.from(this).build();
        dbSession.select(query);
        dbSession.close();
    }

    protected abstract K map(RsMapper rs) throws SQLException;

    protected abstract Map<String, Object> mapInsert(K model) throws SQLException;

    public abstract List<Column<?>> getPrimaryKeys();

    public abstract List<Column<?>> getUniqueKeys();

    public abstract String name();
}




















