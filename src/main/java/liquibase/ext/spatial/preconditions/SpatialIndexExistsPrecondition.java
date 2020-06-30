package liquibase.ext.spatial.preconditions;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.visitor.ChangeExecListener;
import liquibase.database.Database;
import liquibase.database.core.DerbyDatabase;
import liquibase.database.core.H2Database;
import liquibase.exception.PreconditionErrorException;
import liquibase.exception.PreconditionFailedException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.ext.spatial.xml.XmlConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.precondition.AbstractPrecondition;
import liquibase.precondition.Precondition;
import liquibase.precondition.core.IndexExistsPrecondition;
import liquibase.precondition.core.TableExistsPrecondition;
import liquibase.resource.ResourceAccessor;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.Index;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import liquibase.util.StringUtils;

/**
 * <code>SpatialIndexExistsPrecondition</code> determines if a spatial index exists on a specified
 * table.
 */
public class SpatialIndexExistsPrecondition extends AbstractPrecondition {
   private String catalogName;
   private String schemaName;
   private String tableName;
   private String columnNames;
   private String indexName;

   public String getCatalogName() {
      return this.catalogName;
   }

   public void setCatalogName(final String catalogName) {
      this.catalogName = catalogName;
   }

   public String getSchemaName() {
      return this.schemaName;
   }

   public void setSchemaName(final String schemaName) {
      this.schemaName = schemaName;
   }

   public String getTableName() {
      return this.tableName;
   }

   public void setTableName(final String tableName) {
      this.tableName = tableName;
   }

   public String getIndexName() {
      return this.indexName;
   }

   public void setIndexName(final String indexName) {
      this.indexName = indexName;
   }

   public String getColumnNames() {
      return this.columnNames;
   }

   public void setColumnNames(final String columnNames) {
      this.columnNames = columnNames;
   }

   @Override
   public String getName() {
      return "spatialIndexExists";
   }

   @Override
   public Warnings warn(final Database database) {
      return new Warnings();
   }

   @Override
   public ValidationErrors validate(final Database database) {
      final ValidationErrors validationErrors;

      if ((database instanceof DerbyDatabase || database instanceof H2Database)
            && getTableName() == null) {
         validationErrors = new ValidationErrors();
         validationErrors
               .addError("tableName is required for " + database.getDatabaseProductName());
      } else {
         final IndexExistsPrecondition precondition = new IndexExistsPrecondition();
         precondition.setCatalogName(getCatalogName());
         precondition.setSchemaName(getSchemaName());
         precondition.setTableName(getTableName());
         precondition.setIndexName(getIndexName());
         precondition.setColumnNames(getColumnNames());
         validationErrors = precondition.validate(database);
      }
      return validationErrors;
   }

   @Override
   public void check(Database database, DatabaseChangeLog changeLog, ChangeSet changeSet, ChangeExecListener changeExecListener) throws PreconditionFailedException, PreconditionErrorException {
      Precondition delegatedPrecondition;
      if (database instanceof DerbyDatabase || database instanceof H2Database) {
         final TableExistsPrecondition precondition = new TableExistsPrecondition();
         precondition.setCatalogName(getCatalogName());
         precondition.setSchemaName(getSchemaName());
         final String tableName = getHatboxTableName();
         precondition.setTableName(tableName);
         delegatedPrecondition = precondition;
      } else {
         final IndexExistsPrecondition precondition = new IndexExistsPrecondition();
         precondition.setCatalogName(getCatalogName());
         precondition.setSchemaName(getSchemaName());
         precondition.setTableName(getTableName());
         precondition.setIndexName(getIndexName());
         precondition.setColumnNames(getColumnNames());
         delegatedPrecondition = precondition;
      }
      delegatedPrecondition.check(database, changeLog, changeSet, changeExecListener);
   }

   /**
    * Generates the table name containing the Hatbox index.
    *
    * @return the Hatbox table name.
    */
   protected String getHatboxTableName() {
      final String tableName;
      if (!StringUtils.hasUpperCase(getTableName())) {
         tableName = getTableName() + "_hatbox";
      } else {
         tableName = getTableName() + "_HATBOX";
      }
      return tableName;
   }

   /**
    * Creates an example of the database object for which to check.
    *
    * @param database
    *           the database instance.
    * @param tableName
    *           the table name of the index.
    * @return the database object example.
    */
   public DatabaseObject getExample(final Database database, final String tableName) {
      final Schema schema = new Schema(getCatalogName(), getSchemaName());
      final DatabaseObject example;

      // For GeoDB, the index is another table.
      if (database instanceof DerbyDatabase || database instanceof H2Database) {
         final String correctedTableName = database.correctObjectName(getHatboxTableName(),
               Table.class);
         example = new Table().setName(correctedTableName).setSchema(schema);
      } else {
         example = getIndexExample(database, schema, tableName);
      }
      return example;
   }

   /**
    * Generates the {@link Index} example (taken from {@link IndexExistsPrecondition}).
    *
    * @param database
    *           the database instance.
    * @param schema
    *           the schema instance.
    * @param tableName
    *           the table name of the index.
    * @return the index example.
    */
   protected Index getIndexExample(final Database database, final Schema schema,
         final String tableName) {
      final Index example = new Index();
      if (tableName != null) {
         example.setTable((Table) new Table().setName(
               database.correctObjectName(getTableName(), Table.class)).setSchema(schema));
      }
      example.setName(database.correctObjectName(getIndexName(), Index.class));
      if (StringUtils.trimToNull(getColumnNames()) != null) {
         for (final String columnName : getColumnNames().split("\\s*,\\s*")) {
            final Column column = new Column(database.correctObjectName(columnName, Column.class));
            example.getColumns().add(column);
         }
      }
      return example;
   }

   /**
    * @see liquibase.serializer.LiquibaseSerializable#getSerializedObjectName()
    */
   @Override
   public String getSerializedObjectName() {
      return "spatialIndexExists";
   }

   /**
    * @see liquibase.serializer.LiquibaseSerializable#getSerializableFields()
    */
   @Override
   public Set<String> getSerializableFields() {
      return new LinkedHashSet<String>(Arrays.asList("catalogName", "schemaName", "tableName",
            "columnNames", "indexName"));
   }

   /**
    * @see liquibase.serializer.LiquibaseSerializable#getSerializableFieldValue(java.lang.String)
    */
   @Override
   public Object getSerializableFieldValue(final String field) {
      final Object value;
      if ("catalogName".equals(field)) {
         value = getCatalogName();
      } else if ("schemaName".equals(field)) {
         value = getSchemaName();
      } else if ("tableName".equals(field)) {
         value = getTableName();
      } else if ("columnNames".equals(field)) {
         value = getColumnNames();
      } else if ("indexName".equals(field)) {
         value = getIndexName();
      } else {
         throw new UnexpectedLiquibaseException("Unexpected field request on "
               + getSerializedObjectName() + ": " + field);
      }
      return value;
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
      final String namespace = getSerializedObjectNamespace();
      final ParsedNode node = new ParsedNode(namespace, getSerializedObjectName());
      for (final String field : getSerializableFields()) {
         final Object value = getSerializableFieldValue(field);
         node.addChild(namespace, field, value);
      }
      return node;
   }

   /**
    * @see liquibase.precondition.Precondition#load(liquibase.parser.core.ParsedNode,
    *      liquibase.resource.ResourceAccessor)
    */
   @Override
   public void load(final ParsedNode parsedNode, final ResourceAccessor resourceAccessor)
         throws ParsedNodeException {
      final String namespace = null;
      this.catalogName = parsedNode.getChildValue(namespace, "catalogName", String.class);
      this.schemaName = parsedNode.getChildValue(namespace, "schemaName", String.class);
      this.tableName = parsedNode.getChildValue(namespace, "tableName", String.class);
      this.columnNames = parsedNode.getChildValue(namespace, "columnNames", String.class);
      this.indexName = parsedNode.getChildValue(namespace, "indexName", String.class);
   }
}
