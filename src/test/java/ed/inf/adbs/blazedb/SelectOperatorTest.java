package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.SelectOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.operator.JoinOperator;
import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.LongValue;

public class SelectOperatorTest {
    public static void main(String[] args) {
        System.out.println("Running SelectOperator Tests...");

        // Set up database catalogue
        String schemaDirectory = "samples";  // Ensure correct schema directory
        DBCatalogue dbCatalogue = new DBCatalogue(schemaDirectory);

        // Define Student and Enrolled tables
        Table studentTable = new Table("Student");
        Table enrolledTable = new Table("Enrolled");

        // Ensure both tables exist
        if (!dbCatalogue.tableExists("Student") || !dbCatalogue.tableExists("Enrolled")) {
            System.err.println("Error: One or both tables do not exist in schema!");
            return;
        }

        // Initialize ScanOperators for Student and Enrolled tables
        ScanOperator studentScan = new ScanOperator(studentTable, dbCatalogue);
        ScanOperator enrolledScan = new ScanOperator(enrolledTable, dbCatalogue);

        // ==================== CASE 1: Student.C > 100 ====================
        System.out.println("Testing getNextTuple() method with condition 'Student.C > 100'...");

        // Define condition: Student.C > 100
        GreaterThan greaterThanCCondition = new GreaterThan();
        greaterThanCCondition.setLeftExpression(new Column(studentTable, "C")); // Student.C
        greaterThanCCondition.setRightExpression(new LongValue(100)); // 100

        // Initialize SelectOperator with condition
        SelectOperator selectOperator1 = new SelectOperator(studentScan, greaterThanCCondition);

        // Fetch and print filtered tuples
        Tuple tuple;
        while ((tuple = selectOperator1.getNextTuple()) != null) {
            System.out.println("Filtered Tuple (Student.C > 100): " + tuple);
        }

        // Reset scan operator for next test
        studentScan.reset();

        // ==================== CASE 2: Student.C < 100 AND Student.A > 100 ====================
        System.out.println("Testing with condition 'Student.C < 100 AND Student.A > 2'...");

        // Define condition: Student.C < 100
        MinorThan minorThanCCondition = new MinorThan();
        minorThanCCondition.setLeftExpression(new Column(studentTable, "C")); // Student.C
        minorThanCCondition.setRightExpression(new LongValue(100)); // 100

        // Define condition: Student.A > 100
        GreaterThan greaterThanACondition = new GreaterThan();
        greaterThanACondition.setLeftExpression(new Column(studentTable, "A")); // Student.A
        greaterThanACondition.setRightExpression(new LongValue(2)); // 2

        // Combine conditions with AND
        AndExpression combinedCondition = new AndExpression(minorThanCCondition, greaterThanACondition);

        // Initialize SelectOperator with combined condition
        SelectOperator selectOperator2 = new SelectOperator(studentScan, combinedCondition);

        // Fetch and print filtered tuples
        while ((tuple = selectOperator2.getNextTuple()) != null) {
            System.out.println("Filtered Tuple (Student.C < 100 AND Student.A > 100): " + tuple);
        }

        // Reset scan operator before next test
        studentScan.reset();

        // ==================== CASE 3: 1 = 1 (All Tuples Should Pass) ====================
        System.out.println("Testing with condition '1 = 1' after join...");

        // Define join condition: Student.A = Enrolled.A
        EqualsTo joinCondition = new EqualsTo();
        joinCondition.setLeftExpression(new Column(studentTable, "A")); // Student.A
        joinCondition.setRightExpression(new Column(enrolledTable, "A")); // Enrolled.A

        // Initialize JoinOperator
        JoinOperator joinOperator = new JoinOperator(studentScan, enrolledScan, joinCondition);

        // Reset joinOperator to avoid stale data
        studentScan.reset();
        enrolledScan.reset();
        joinOperator.reset();

        // Define universal condition (1=1)
        EqualsTo alwaysTrueCondition = new EqualsTo();
        alwaysTrueCondition.setLeftExpression(new LongValue(1)); // 1
        alwaysTrueCondition.setRightExpression(new LongValue(1)); // 1

        // Initialize SelectOperator with always-true condition
        SelectOperator selectOperator3 = new SelectOperator(joinOperator, alwaysTrueCondition);

        // Fetch and print all tuples (should return everything from the join)
        while ((tuple = selectOperator3.getNextTuple()) != null) {
            System.out.println("Filtered Tuple (1=1, All Tuples Pass): " + tuple);
        }

        System.out.println("SelectOperator Tests completed successfully!");
    }
}
