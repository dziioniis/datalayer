package com.lambda.collect.datalayer.builder;

import com.lambda.collect.datalayer.builder.types.Order;
import com.lambda.collect.datalayer.db.Db;
import com.lambda.collect.datalayer.db.DbSession;
import com.lambda.collect.datalayer.tables.MigrationsTable;
import com.lambda.collect.datalayer.types.ColumnType;
import com.lambda.collect.datalayer.types.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class Migrator {
    public final Db db;
    private SessionWrapper sessionWrapper = new SessionWrapper();
    private List<String> queries = new ArrayList<>();

    private boolean shouldCallDb = true;

    private final Map<Integer, Callable<Boolean>> migrationsPerVersion = new HashMap<>();

    @Autowired
    public Migrator(Db db) {
        this.db = db;
    }

    public class SessionWrapper {
        public void execute(String query) throws SQLException {
            queries.add(query);
            if(shouldCallDb) {
                db.getDbSession().execute(query);
            }
        }

        public void update(String query) throws SQLException {
            queries.add(query);
            if(shouldCallDb) {
                db.getDbSession().update(query);
            }
        }
    }

    public <T extends ColumnType> Migrator chain(int version, CustomRunnable migration) {
        Callable<Boolean> callable = () -> {
            migration.call(sessionWrapper);
            return true;
        };

        this.migrationsPerVersion.put(version, callable);
        return this;
    }

    public MigrationsTable.Model getLastOrNull(List<MigrationsTable.Model> list) {
        MigrationsTable.Model latestMigration = null;
        if (list.size() != 0) {
            latestMigration = list.get(list.size() - 1);
        }
        return latestMigration;
    }

    public String asSql() {
        this.shouldCallDb = false;

        for (Map.Entry<Integer, Callable<Boolean>> entry : this.migrationsPerVersion.entrySet()) {
            try {
                entry.getValue().call();
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }

        this.shouldCallDb = true;

        return String.join("\n\n", queries);
    }

    public void execute() throws Exception {
        MigrationsTable migrationsTable = MigrationsTable.INSTANCE;

        db.use(migrationsTable);
        DbSession dbSession = db.getDbSession();

        String migrationsCreate = Create.from(migrationsTable).build();
        dbSession.execute(migrationsCreate);
        dbSession.close();

        List<MigrationsTable.Model> migrations = migrationsTable
                .selectAll(new Order<>(Collections.singletonList(migrationsTable.id), SortOrder.ASC));

        final MigrationsTable.Model latestMigration = getLastOrNull(migrations);

        Map<Integer, Callable<Boolean>> queriesToExecute = this.migrationsPerVersion;
        if (latestMigration != null) {
            queriesToExecute = this.migrationsPerVersion.entrySet()
                    .stream()
                    .filter(it -> it.getKey() > latestMigration.version)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        for (Map.Entry<Integer, Callable<Boolean>> entry : queriesToExecute.entrySet()) {
            //execute all queries. If failed - rollback
            Transaction transaction = new Transaction(dbSession);
            transaction.begin();
            try {
                entry.getValue().call();
            } catch (Exception e) {
                e.printStackTrace();
                transaction.rollback("");
                break;
            }
            transaction.commit();
            //add new version row to  db
            migrationsTable.insert(new MigrationsTable.Model(entry.getKey(), new Timestamp(new Date().getTime())));
        }
    }

    public interface CustomRunnable {
        void call(SessionWrapper dbSession) throws Exception;
    }

    public static void main(String[] args) throws Exception {
    }
}
