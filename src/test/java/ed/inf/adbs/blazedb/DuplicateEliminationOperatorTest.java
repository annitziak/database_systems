package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.DuplicateEliminationOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import net.sf.jsqlparser.schema.Table;

import java.io.File;

public class DuplicateEliminationOperatorTest {
    public static void main(String[] args) {
        System.out.println("Running DuplicateEliminationOperator Tests...");

        // Set up database catalogue and table
        String schemaDirectory = "samples" + File.separator + "db";  // Ensure correct schema directory
        DBCatalogue dbCatalogue = new DBCatalogue(schemaDirectory);
        Table table = new Table("Student");

        // Ensure the table exists
        if (!dbCatalogue.tableExists("Student")) {
            System.err.println("Table 'Student' does not exist in schema!");
            return;
        }

        // Initialize ScanOperator as the child operator
        ScanOperator scanOperator = new ScanOperator(table, dbCatalogue);

        // Pass ScanOperator to DuplicateEliminationOperator
        DuplicateEliminationOperator deOperator = new DuplicateEliminationOperator(scanOperator, "Student.C");

        // Read and print unique tuples
        System.out.println("Testing getNextTuple() method...");
        Tuple tuple;
        while ((tuple = deOperator.getNextTuple()) != null) {
            System.out.println("Unique Tuple: " + tuple);
        }

        // correct if it skipped the duplicate values

        // Reset and test again
        System.out.println("Testing reset() method...");
        deOperator.reset();

        Tuple resetTuple = deOperator.getNextTuple();
        System.out.println("First tuple after reset: " + resetTuple);

        System.out.println("DuplicateEliminationOperator Tests completed successfully!");
    }
}
