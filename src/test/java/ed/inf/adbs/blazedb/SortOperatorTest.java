package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.operator.SortOperator;
import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.schema.Column;

import java.io.BufferedReader;

import java.io.File;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

public class SortOperatorTest {
    public static void main(String[] args) {
        System.out.println("Running SortOperator Tests...");

        // Set up database catalogue and table
        String schemaDirectory = "samples" + File.separator +"db";
        DBCatalogue dbCatalogue = new DBCatalogue(schemaDirectory);
        Table table = new Table("Student");

        // Ensure the table exists
        if (!dbCatalogue.tableExists("Student")) {
            System.err.println("Table 'Student' does not exist in schema!");
            return;
        }

        // Initialize ScanOperator
        ScanOperator scanOperator = new ScanOperator(table, dbCatalogue);


        // Define sorting columns (Sorting by column 'C' first, then by 'B')
        OrderByElement orderByC = new OrderByElement();
        orderByC.setExpression(new Column(table, "C"));
        OrderByElement orderByB = new OrderByElement();
        orderByB.setExpression(new Column(table, "B"));
        List<OrderByElement> orderByElements = Arrays.asList(orderByB, orderByC);


        // Create SortOperator with ScanOperator as child
        SortOperator sortOperator = new SortOperator(scanOperator, orderByElements);

        // Read sorted tuples : this will be the output of the SortOperator
        System.out.println("Testing getNextTuple() method...");
        Tuple tuple;
        while ((tuple = sortOperator.getNextTuple()) != null) {
            System.out.println("Sorted Tuple: " + tuple);
        }

        // Test reset functionality
        System.out.println("Testing reset() method...");
        sortOperator.reset();

        Tuple resetTuple = sortOperator.getNextTuple();
        System.out.println("First sorted tuple after reset: " + resetTuple);

        System.out.println("SortOperator Tests completed successfully!");
    }
}