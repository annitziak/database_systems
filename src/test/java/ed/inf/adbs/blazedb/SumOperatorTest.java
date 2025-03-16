package ed.inf.adbs.blazedb;

import ed.inf.adbs.blazedb.operator.SumOperator;
import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.GroupByElement;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SumOperatorTest {
    public static void main(String[] args) {
        System.out.println("Running SumOperator Tests...");

        // Set up database catalogue
        String schemaDirectory = "samples" + File.separator + "db";
        DBCatalogue dbCatalogue = new DBCatalogue(schemaDirectory);

        // Define Student table
        Table studentTable = new Table("Student");

        // Ensure table exists
        if (!dbCatalogue.tableExists("Student")) {
            System.err.println("Error: Table 'Student' does not exist in schema!");
            return;
        }

        // Initialize ScanOperator for Student table
        ScanOperator studentScan = new ScanOperator(studentTable, dbCatalogue);

        // ==================== CASE 1: SUM(Student.C) WITHOUT GROUP BY ====================
        System.out.println("\nTesting SUM(Student.C) without GROUP BY...");

        List<Function> sumFunctions1 = Collections.singletonList(createSumFunction(studentTable, "C"));

        SumOperator sumOperator1 = new SumOperator(studentScan, null, sumFunctions1);

        printResults(sumOperator1, "SUM(Student.C)");

        studentScan.reset(); // Reset for next test

        // ==================== CASE 2: SUM(1) WITHOUT GROUP BY ====================
        System.out.println("\nTesting SUM(1) without GROUP BY...");

        List<Function> sumFunctions2 = Collections.singletonList(createSumOneFunction());

        SumOperator sumOperator2 = new SumOperator(studentScan, null, sumFunctions2);

        printResults(sumOperator2, "SUM(1)");

        studentScan.reset(); // Reset for next test

        // ==================== CASE 3: SUM(1), SUM(Student.A) WITHOUT GROUP BY ====================
        System.out.println("\nTesting SUM(1), SUM(Student.A) without GROUP BY...");

        List<Function> sumFunctions3 = Arrays.asList(createSumOneFunction(), createSumFunction(studentTable, "A"));

        SumOperator sumOperator3 = new SumOperator(studentScan, null, sumFunctions3);

        printResults(sumOperator3, "SUM(1), SUM(Student.A)");

        studentScan.reset(); // Reset for next test

        // ==================== CASE 4: SUM(Student.B) WITH GROUP BY (Student.A) ====================
        System.out.println("\nTesting SUM(Student.B) with GROUP BY Student.A...");

        List<Function> sumFunctions4 = Collections.singletonList(createSumFunction(studentTable, "B"));
        GroupByElement groupByElement = createGroupByElement(studentTable, "A");

        SumOperator sumOperator4 = new SumOperator(studentScan, groupByElement, sumFunctions4);

        printResults(sumOperator4, "SUM(Student.B) GROUP BY Student.A");

        // ==================== CASE 5: SUM(Student.B), SUM(1) WITH GROUP BY (Student.A) ====================
        System.out.println("\nTesting SUM(Student.B), SUM(1) with GROUP BY Student.A...");

        List<Function> sumFunctions5 = Arrays.asList(createSumFunction(studentTable, "B"), createSumOneFunction());
        GroupByElement groupByElement5 = createGroupByElement(studentTable, "A");

        SumOperator sumOperator5 = new SumOperator(studentScan, groupByElement5, sumFunctions5);

        printResults(sumOperator5, "SUM(Student.B), SUM(1) GROUP BY Student.A");


        // ==================== CASE 6: Student.A WITH GROUP BY (Student.A) ====================
        System.out.println("\nTesting Student.A with GROUP BY Student.A,");
        List<Function> sumFunctions6 = Collections.singletonList(createSumFunction(studentTable, "A"));
        GroupByElement groupByElement6 = createGroupByElement(studentTable, "A");

        SumOperator sumOperator6 = new SumOperator(studentScan, groupByElement6, sumFunctions6);

        printResults(sumOperator6, "Student.A GROUP BY Student.A");

    }

    /**
     * Helper method to create a SUM function for a given column.
     */
    private static Function createSumFunction(Table table, String columnName) {
        Function sumFunction = new Function();
        sumFunction.setName("SUM");
        sumFunction.setParameters(new ExpressionList(Collections.singletonList(new Column(table, columnName))));
        return sumFunction;
    }

    /**
     * Helper method to create a SUM(1) function.
     */
    private static Function createSumOneFunction() {
        Function sumOneFunction = new Function();
        sumOneFunction.setName("SUM");
        sumOneFunction.setParameters(new ExpressionList(Collections.singletonList(new net.sf.jsqlparser.expression.LongValue(1))));
        return sumOneFunction;
    }

    /**
     * Helper method to create a GROUP BY element for a given column.
     */
    private static GroupByElement createGroupByElement(Table table, String columnName) {
        GroupByElement groupByElement = new GroupByElement();
        ExpressionList expressionList = new ExpressionList();
        expressionList.setExpressions(Collections.singletonList(new Column(table, columnName)));
        groupByElement.setGroupByExpressions(expressionList);
        return groupByElement;
    }

    /**
     * Helper method to print results from a SumOperator.
     */
    private static void printResults(SumOperator sumOperator, String testCase) {
        Tuple tuple;
        System.out.println("Results for " + testCase + ":");
        boolean hasResults = false;
        while ((tuple = sumOperator.getNextTuple()) != null) {
            hasResults = true;
            System.out.println(tuple);
        }
        if (!hasResults) {
            System.out.println("No results found.");
        }
    }
}
