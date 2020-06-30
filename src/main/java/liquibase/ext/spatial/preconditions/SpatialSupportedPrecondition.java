package liquibase.ext.spatial.preconditions;

import java.util.LinkedHashSet;
import java.util.Set;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.visitor.ChangeExecListener;
import liquibase.database.Database;
import liquibase.database.core.DerbyDatabase;
import liquibase.database.core.H2Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.core.OracleDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.PreconditionErrorException;
import liquibase.exception.PreconditionFailedException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.executor.ExecutorService;
import liquibase.ext.spatial.xml.XmlConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.precondition.AbstractPrecondition;
import liquibase.precondition.ErrorPrecondition;
import liquibase.precondition.core.TableExistsPrecondition;
import liquibase.precondition.core.ViewExistsPrecondition;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.core.RawSqlStatement;

/**
 * <code>SpatialSupportedPrecondition</code> checks the state of the database and determines if it
 * has spatial support.
 *
 * @author Lonny Jacobson
 */
public class SpatialSupportedPrecondition extends AbstractPrecondition {
   @Override
   public String getName() {
      return "spatialSupported";
   }

   @Override
   public Warnings warn(final Database database) {
      final Warnings warnings = new Warnings();
      if (!(database instanceof DerbyDatabase || database instanceof H2Database
            || database instanceof MySQLDatabase || database instanceof OracleDatabase || database instanceof PostgresDatabase)) {
         warnings.addWarning(database.getDatabaseProductName()
               + " is not supported by this extension");
      }
      return warnings;
   }

   @Override
   public ValidationErrors validate(final Database database) {
      final ValidationErrors errors = new ValidationErrors();
      if (!(database instanceof DerbyDatabase || database instanceof H2Database
            || database instanceof MySQLDatabase || database instanceof OracleDatabase || database instanceof PostgresDatabase)) {
         errors.addError(database.getDatabaseProductName() + " is not supported by this extension");
      }
      return errors;
   }




   @Override
   public void check(Database database, DatabaseChangeLog changeLog, ChangeSet changeSet, ChangeExecListener changeExecListener) throws PreconditionFailedException, PreconditionErrorException {
      if (database instanceof DerbyDatabase || database instanceof H2Database) {
         final TableExistsPrecondition precondition = new TableExistsPrecondition();
         precondition.setTableName("geometry_columns");
         precondition.check(database, changeLog, changeSet, changeExecListener);
      } else if (database instanceof PostgresDatabase) {
         final ViewExistsPrecondition precondition = new ViewExistsPrecondition();
         precondition.setSchemaName("public");
         precondition.setViewName("geometry_columns");
         precondition.check(database, changeLog, changeSet, changeExecListener);
      } else if (database instanceof OracleDatabase) {
         // Explicitly query the database due to CORE-2198.
         final RawSqlStatement sql = new RawSqlStatement(
               "SELECT count(*) FROM ALL_VIEWS WHERE upper(VIEW_NAME)='USER_SDO_GEOM_METADATA' AND OWNER='MDSYS'");
         try {
            final Integer result = ExecutorService.getInstance().getExecutor(database)
                  .queryForObject(sql, Integer.class);
            if (result == null || result.intValue() == 0) {
               throw new PreconditionFailedException(
                     "The view MDSYS.USER_SDO_GEOM_METADATA does not exist. This database is not spatially enabled",
                     changeLog, this);
            }
         } catch (final DatabaseException e) {
            throw new PreconditionErrorException(e, changeLog, this);
         }
      } else if (!(database instanceof MySQLDatabase)) {
         final Throwable exception = new LiquibaseException(database.getDatabaseProductName()
               + " is not supported by this extension");
         final ErrorPrecondition errorPrecondition = new ErrorPrecondition(exception, changeLog,
               this);
         throw new PreconditionErrorException(errorPrecondition);
      }
   }

   /**
    * @see liquibase.serializer.LiquibaseSerializable#getSerializedObjectName()
    */
   @Override
   public String getSerializedObjectName() {
      return getName();
   }

   /**
    * @see liquibase.serializer.LiquibaseSerializable#getSerializableFields()
    */
   @Override
   public Set<String> getSerializableFields() {
      return new LinkedHashSet<String>();
   }

   /**
    * @see liquibase.serializer.LiquibaseSerializable#getSerializableFieldValue(java.lang.String)
    */
   @Override
   public Object getSerializableFieldValue(final String field) {
      throw new UnexpectedLiquibaseException("Unexpected field request on "
            + getSerializedObjectName() + ": " + field);
   }

   /**
    * @see liquibase.serializer.LiquibaseSerializable#getSerializableFieldType(java.lang.String)
    */
   @Override
   public SerializationType getSerializableFieldType(final String field) {
      return SerializationType.NAMED_FIELD;
   }

   /**
    * @see liquibase.serializer.LiquibaseSerializable#getSerializedObjectNamespace()
    */
   @Override
   public String getSerializedObjectNamespace() {
      return XmlConstants.SPATIAL_CHANGELOG_NAMESPACE;
   }

   /**
    * @see liquibase.serializer.LiquibaseSerializable#serialize()
    */
   @Override
   public ParsedNode serialize() throws ParsedNodeException {
      return new ParsedNode(getSerializedObjectNamespace(), getSerializedObjectName());
   }

   /**
    * @see liquibase.precondition.Precondition#load(liquibase.parser.core.ParsedNode,
    *      liquibase.resource.ResourceAccessor)
    */
   @Override
   public void load(final ParsedNode parsedNode, final ResourceAccessor resourceAccessor)
         throws ParsedNodeException {
   }
}
