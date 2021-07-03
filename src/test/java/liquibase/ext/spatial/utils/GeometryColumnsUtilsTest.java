package liquibase.ext.spatial.utils;

import static org.testng.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.core.H2Database;
import liquibase.database.jvm.JdbcConnection;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * This class tests {@link GeometryColumnsUtils}.
 */
public class GeometryColumnsUtilsTest {
   /**
    * Tests {@link GeometryColumnsUtils#geometryColumnsExists(Database)}.
    * 
    * @param database
    *           the database to test.
    * @param expected
    *           indicates if the table exists.
    */
   @Test(dataProvider = "geometryColumnsExistsTestData")
   public void testGeometryColumnsExists(Database database, boolean expected) {
      boolean result = GeometryColumnsUtils.geometryColumnsExists(database);
      assertEquals(result, expected);
   }

   @DataProvider
   public Object[][] geometryColumnsExistsTestData() throws SQLException {
      // Create an H2 database instance without geometry_columns table.
      final Database noGeometryColumnsDatabase = new H2Database();
      noGeometryColumnsDatabase.setConnection(new JdbcConnection(DriverManager
            .getConnection("jdbc:h2:mem:target/noGeometryColumns")));

      final Database geometryColumnsDatabase = new H2Database();
      Connection connection = DriverManager
            .getConnection("jdbc:h2:mem:target/geometryColumns");
      DatabaseConnection conn = new JdbcConnection(connection);
      geometryColumnsDatabase.setConnection(conn);

      Statement statement = connection.createStatement();
      statement.execute("CREATE ALIAS IF NOT EXISTS H2GIS_SPATIAL FOR \"org.h2gis.functions.factory.H2GISFunctions.load\"");
      statement.execute("CALL H2GIS_SPATIAL()");
      statement.close();
      return new Object[][] {
            new Object[] { noGeometryColumnsDatabase, false },
            new Object[] { geometryColumnsDatabase, true } };
   }
}
