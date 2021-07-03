package liquibase.ext.spatial.change;

import java.util.ArrayList;
import java.util.List;

import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.ChangeWithColumns;
import liquibase.change.ColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.spatial.statement.CreateSpatialIndexStatement;
import liquibase.ext.spatial.xml.XmlConstants;
import liquibase.statement.SqlStatement;
import liquibase.util.StringUtils;

/**
 * The <code>CreateSpatialIndexChange</code> represents a database change to
 * create a spatial index.
 */
@DatabaseChange(name = "createSpatialIndex",
      description = "Creates a spatial index on an existing column or set of columns.",
      priority = ChangeMetaData.PRIORITY_DEFAULT,
      appliesTo = "index")
public class CreateSpatialIndexChange extends AbstractChange
      implements ChangeWithColumns<ColumnConfig> {
   private String catalogName;
   private String schemaName;
   private String tableName;
   private String indexName;
   private String tablespace;
   private List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
   private String geometryType;
   private String srid;

   /**
    * Sets the database catalog name.
    *
    * @param catalogName
    *           the catalog name.
    */
   public void setCatalogName(final String catalogName) {
      this.catalogName = catalogName;
   }

   @DatabaseChangeProperty(description = "Name of the catalog")
   public String getCatalogName() {
      return this.catalogName;
   }

   @DatabaseChangeProperty(mustEqualExisting = "index",
         description = "Name of the index to create",
         requiredForDatabase = "mysql, oracle, postgresql")
   public String getIndexName() {
      return this.indexName;
   }

   public void setIndexName(final String indexName) {
      this.indexName = indexName;
   }

   @DatabaseChangeProperty(mustEqualExisting = "index.schema")
   public String getSchemaName() {
      return this.schemaName;
   }

   public void setSchemaName(final String schemaName) {
      this.schemaName = schemaName;
   }

   @DatabaseChangeProperty(mustEqualExisting = "index.table",
         description = "Name of the table to add the index to",
         exampleValue = "person",
         requiredForDatabase = "all")
   public String getTableName() {
      return this.tableName;
   }

   public void setTableName(final String tableName) {
      this.tableName = tableName;
   }

   /**
    * Returns the geometry type.
    *
    * @return the geometry type.
    */
   @DatabaseChangeProperty(description = "The Well-Known Text geometry type",
         exampleValue = "POINT")
   public String getGeometryType() {
      return this.geometryType;
   }

   /**
    * Sets the geometry type.
    *
    * @param geometryType
    *           the geometry type.
    */
   public void setGeometryType(final String geometryType) {
      this.geometryType = geometryType;
   }

   /**
    * Returns the srid.
    *
    * @return the srid.
    */
   @DatabaseChangeProperty(
         description = "The Spatial Reference ID of the indexed data.  An EPSG SRID is assumed.",
         exampleValue = "4326",
         requiredForDatabase = {"derby", "h2"})
   public String getSrid() {
      return this.srid;
   }

   /**
    * Sets the srid.
    *
    * @param srid
    *           the srid.
    */
   public void setSrid(final String srid) {
      this.srid = srid;
   }

   @Override
   @DatabaseChangeProperty(mustEqualExisting = "index.column",
         description = "Column(s) to add to the index",
         requiredForDatabase = "all")
   public List<ColumnConfig> getColumns() {
      if (this.columns == null) {
         return new ArrayList<ColumnConfig>();
      }
      return this.columns;
   }

   @Override
   public void setColumns(final List<ColumnConfig> columns) {
      this.columns = columns;
   }

   @Override
   public void addColumn(final ColumnConfig column) {
      this.columns.add(column);
   }

   @DatabaseChangeProperty(description = "Tablepace to create the index in.")
   public String getTablespace() {
      return this.tablespace;
   }

   public void setTablespace(final String tablespace) {
      this.tablespace = tablespace;
   }

   /**
    * @see liquibase.change.AbstractChange#validate(liquibase.database.Database)
    */
   @Override
   public ValidationErrors validate(final Database database) {
      final ValidationErrors validationErrors = new ValidationErrors();
      if (this.srid != null) {
         if (!this.srid.matches("[0-9]+")) {
            validationErrors.addError("The SRID must be numeric");
         }
      }

      if (!validationErrors.hasErrors()) {
         validationErrors.addAll(super.validate(database));
      }

      return validationErrors;
   }

   @Override
   public String getConfirmationMessage() {
      final StringBuilder message = new StringBuilder("Spatial index");
      if (StringUtils.trimToNull(getIndexName()) != null) {
         message.append(' ').append(getIndexName().trim());
      }
      message.append(" created");
      if (StringUtils.trimToNull(getTableName()) != null) {
         message.append(" on ").append(getTableName().trim());
      }
      return message.toString();
   }

   @Override
   public SqlStatement[] generateStatements(final Database database) {
      final String[] columns = new String[this.columns.size()];
      int ii = 0;
      for (final ColumnConfig columnConfig : this.columns) {
         columns[ii++] = columnConfig.getName();
      }

      // Parse the string SRID into an integer.
      Integer srid = null;
      if (getSrid() != null) {
         srid = Integer.valueOf(getSrid());
      }

      final CreateSpatialIndexStatement statement = new CreateSpatialIndexStatement(
            getIndexName(), getCatalogName(), getSchemaName(), getTableName(), columns,
            getTablespace(), getGeometryType(), srid);
      return new SqlStatement[] { statement };
   }

   @Override
   protected Change[] createInverses() {
      final DropSpatialIndexChange inverse = new DropSpatialIndexChange();
      inverse.setCatalogName(getCatalogName());
      inverse.setSchemaName(getSchemaName());
      inverse.setTableName(getTableName());
      inverse.setIndexName(getIndexName());

      return new Change[] { inverse };
   }

   @Override
   public String getSerializedObjectNamespace() {
      return XmlConstants.SPATIAL_CHANGELOG_NAMESPACE;
   }
}
