package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;


public class ScanOperatorTest {
    public static void main(String[] args) throws IOException {
        System.out.println("Running ScanOperator Tests...");

        // Initialize a mock DBCatalogue
        String schemaDirectory = "samples" + File.separator + "db";
        DBCatalogue dbCatalogue = new DBCatalogue(schemaDirectory);
        FromItem fromItem = new Table("Student");

        // Check if the table exists in the catalog
        if (!dbCatalogue.tableExists("Student")) {
            System.err.println("Table 'Student' does not exist in schema!");
            return;
        }

        // Initialize ScanOperator
        ScanOperator scanOperator = new ScanOperator(fromItem, dbCatalogue);

        // Test reading tuples
        System.out.println("Testing getNextTuple() method...");
        Tuple tuple;
        while ((tuple = scanOperator.getNextTuple()) != null) {
            System.out.println("Read Tuple: " + tuple);
        }

        // Test reset functionality
        System.out.println("Testing reset() method...");
        scanOperator.reset();

        Tuple resetTuple = scanOperator.getNextTuple();
        System.out.println("First tuple after reset: " + resetTuple);

        System.out.println("ScanOperator Tests completed successfully!");
    }
}
