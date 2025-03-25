package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.dbcatalogue.DBStatistics;
import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import java.io.File;
import java.util.List;
import java.util.Map;

public class DBStatisticsTest {
    public static void main(String[] args) {
        System.out.println("Running DBStatistics Test...");

        // Initialize the database catalog
        String schemaDirectory = "samples" + File.separator + "db";
        DBCatalogue dbCatalogue = new DBCatalogue(schemaDirectory);
        DBStatistics dbStatistics = new DBStatistics(dbCatalogue);

        // Retrieve all tables from the schema
        List<String> tableNames = dbCatalogue.getAllTables();
        if (tableNames.isEmpty()) {
            System.err.println("No tables found in the schema!");
            return;
        }

        System.out.println("Computing statistics for tables...");

        // Iterate over all tables and compute statistics
        for (String tableName : tableNames) {
            System.out.println("\n=== Table: " + tableName + " ===");

            // Create a FromItem instance for the table
            FromItem fromItem = new Table(tableName);

            // Compute statistics for the table
            dbStatistics.computeStatistics(fromItem);

            // Retrieve column statistics for this table
            List<String> columnNames = dbCatalogue.getTableColumns(tableName);

            // Print per-column statistics
            boolean hasStats = false;
            for (String column : columnNames) {
                List<Integer> stats = dbStatistics.getColumnStatistics(tableName, column);

                if (stats != null && stats.size() == 4) {
                    hasStats = true;
                    System.out.println("Column: " + column);
                    System.out.println("  Min Value       : " + stats.get(0));
                    System.out.println("  Max Value       : " + stats.get(1));
                    System.out.println("  Distinct Count  : " + stats.get(2));
                    System.out.println("  Number of Tuples: " + stats.get(3));
                }
            }

            if (!hasStats) {
                System.out.println("No statistics computed?");
            }
        }
        System.out.println("\nDBStatistics Test completed successfully!");
    }
}
