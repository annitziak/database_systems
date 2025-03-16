package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.operator.ProjectOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.FromItem;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

public class ProjectOperatorTest {
    public static void main(String[] args) {
        System.out.println("Running ProjectOperator Tests...");

        // Set the directory where schema.txt is already present
        String schemaDirectory = "samples" + File.separator + "db";
        DBCatalogue dbCatalogue = new DBCatalogue(schemaDirectory);
        FromItem fromItem = new Table("Student"); // Table name to scan

        // Check if the table exists in the catalog
        if (!dbCatalogue.tableExists("Student")) {
            System.err.println("Table 'Student' does not exist in schema!");
            return;
        }

        // Initialize ScanOperator as the child operator
        ScanOperator scanOperator = new ScanOperator(fromItem, dbCatalogue);

        // Define projection columns (Selecting only 'A' and 'C' columns)
        List<SelectItem<?>> selectColumns = Arrays.asList(createSelectItem("A"), createSelectItem("C"));

        // Create ProjectOperator with ScanOperator as child
        ProjectOperator projectOperator = new ProjectOperator(scanOperator, selectColumns);

        // Read tuples
        System.out.println("Testing getNextTuple() method...");
        Tuple tuple;
        while ((tuple = projectOperator.getNextTuple()) != null) {
            System.out.println("Projected Tuple: " + tuple);
        }

        // Test reset functionality
        System.out.println("Testing reset() method...");
        projectOperator.reset();

        Tuple resetTuple = projectOperator.getNextTuple();
        System.out.println("First projected tuple after reset: " + resetTuple);

        System.out.println("ProjectOperator Tests completed successfully!");
    }

    private static SelectItem<?> createSelectItem(String columnName) {
        return new SelectItem<>(new net.sf.jsqlparser.schema.Column(null, columnName));
    }

}