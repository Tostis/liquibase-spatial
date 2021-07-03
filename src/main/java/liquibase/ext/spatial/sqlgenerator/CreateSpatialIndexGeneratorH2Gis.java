package liquibase.ext.spatial.sqlgenerator;

import liquibase.database.Database;
import liquibase.database.core.H2Database;
import liquibase.database.core.PostgresDatabase;
import liquibase.ext.spatial.statement.CreateSpatialIndexStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.structure.core.Index;

import java.util.Arrays;
import java.util.Iterator;

/**
 * <code>CreateSpatialIndexGeneratorH2Gis</code> generates the SQL for creating a spatial index in
 * H2 with H2Gis.
 */
public class CreateSpatialIndexGeneratorH2Gis extends AbstractCreateSpatialIndexGenerator {
   @Override
   public boolean supports(final CreateSpatialIndexStatement statement, final Database database) {
      return database instanceof H2Database;
   }

   @Override
   public Sql[] generateSql(final CreateSpatialIndexStatement statement, final Database database,
         final SqlGeneratorChain sqlGeneratorChain) {
      final StringBuilder sql = new StringBuilder();
      sql.append("CREATE INDEX ");
      sql.append(database.escapeObjectName(statement.getIndexName(), Index.class));
      sql.append(" ON ");
      sql.append(database.escapeTableName(statement.getTableCatalogName(),
            statement.getTableSchemaName(), statement.getTableName()));
      sql.append("(");
      final Iterator<String> iterator = Arrays.asList(statement.getColumns()).iterator();
      while (iterator.hasNext()) {
         final String column = iterator.next();
         sql.append(database.escapeColumnName(statement.getTableCatalogName(),
               statement.getTableSchemaName(), statement.getTableName(), column));
         if (iterator.hasNext()) {
            sql.append(", ");
         }
      }
      sql.append(")");
      final Sql createIndex = new UnparsedSql(sql.toString(), getAffectedIndex(statement));
      return new Sql[] { createIndex };
   }
}
