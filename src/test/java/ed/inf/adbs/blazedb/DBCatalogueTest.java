package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;

import java.io.File;
import java.util.List;

public class DBCatalogueTest {
    public static void main(String[] args) {
        System.out.println("Running DBCatalogue Tests with Existing Schema...");

        // Set the directory where schema.txt is already present
        String schemaDirectory = "samples" + File.separator + "db";  // Update this if needed

        // Initialize the DBCatalogue using the existing schema
        DBCatalogue dbCatalogue = new DBCatalogue(schemaDirectory);

        // Test tableExists method with correct capitalization
        System.out.println("Testing tableExists method...");
        System.out.println("Student exists: " + dbCatalogue.tableExists("Student"));  // Corrected case
        System.out.println("Course exists: " + dbCatalogue.tableExists("Course"));    // Corrected case
        System.out.println("Enrolled exists: " + dbCatalogue.tableExists("Enrolled")); // Corrected case
        System.out.println("Teacher exists: " + dbCatalogue.tableExists("Teacher")); // Expected false

        // Test getTableColumns method
        System.out.println("Testing getTableColumns method...");
        printTableColumns(dbCatalogue, "Student");
        printTableColumns(dbCatalogue, "Course");
        printTableColumns(dbCatalogue, "Enrolled");

        // Test getDirectory method
        System.out.println("Testing getDirectory method...");
        System.out.println("Student table path: " + dbCatalogue.getDirectory("Student"));
        System.out.println("Course table path: " + dbCatalogue.getDirectory("Course"));
        System.out.println("Enrolled table path: " + dbCatalogue.getDirectory("Enrolled"));

        System.out.println("Tests completed successfully!");
    }

    private static void printTableColumns(DBCatalogue dbCatalogue, String tableName) {
        List<String> columns = dbCatalogue.getTableColumns(tableName);
        if (columns.isEmpty()) {
            System.out.println("No columns found for table: " + tableName);
        } else {
            System.out.println(tableName + " columns: " + columns);
        }
    }
}