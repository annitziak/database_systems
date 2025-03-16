package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.JoinOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.io.File;

public class JoinOperatorTest {
    public static void main(String[] args) {
        System.out.println("Running JoinOperator Tests...");

        // Set up database catalogue
        String schemaDirectory = "samples"+ File.separator + "db";  // Ensure correct schema directory
        DBCatalogue dbCatalogue = new DBCatalogue(schemaDirectory);

        // Define left and right tables
        Table studentTable = new Table("Student");
        Table enrolledTable = new Table("Enrolled");

        // Ensure tables exist
        if (!dbCatalogue.tableExists("Student") || !dbCatalogue.tableExists("Enrolled")) {
            System.err.println("Error: One or both tables do not exist in schema!");
            return;
        }

        // Initialize ScanOperators for both tables
        ScanOperator studentScan = new ScanOperator(studentTable, dbCatalogue);
        ScanOperator enrolledScan = new ScanOperator(enrolledTable, dbCatalogue);

        // Define Join Condition: Student.A = Enrolled.A
        EqualsTo joinCondition = new EqualsTo();
        joinCondition.setLeftExpression(new Column(studentTable, "A"));  // Student.A
        joinCondition.setRightExpression(new Column(enrolledTable, "A"));  // Enrolled.A


        System.out.println("Join Condition: " + joinCondition);
        // Initialize JoinOperator
        JoinOperator joinOperator = new JoinOperator(studentScan, enrolledScan, joinCondition);

        // Fetch and print joined tuples
        System.out.println("Testing getNextTuple() method...");
        Tuple tuple;
        while ((tuple = joinOperator.getNextTuple()) != null) {
            System.out.println("Joined Tuple: " + tuple);
        }

        // Test reset functionality
        System.out.println("Testing reset() method...");
        joinOperator.reset();

        Tuple resetTuple = joinOperator.getNextTuple();
        System.out.println("First joined tuple after reset: " + resetTuple);

        System.out.println("JoinOperator Tests completed successfully!");
    }
}
