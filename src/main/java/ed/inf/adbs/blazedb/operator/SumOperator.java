package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.GroupByElement;

import java.util.*;


/**
 * SumOperator performs SUM aggregation, with or without GROUP BY.
 */
public class SumOperator extends Operator {

    private final Operator child;
    private final List<Tuple> tuples;
    private Map<List<Object>, List<Tuple>> groups; // Group key -> List of tuples
    private List<Tuple> outputTuples;
    private final List<Function> sumFunctions;

    public SumOperator(Operator child, GroupByElement groupByElement, List<Function> sumFunctions) {
        this.child = child;
        this.tuples = new ArrayList<>();
        this.groups = new HashMap<>();
        this.sumFunctions = sumFunctions;
        this.outputTuples = new ArrayList<>();

        // first we will group by the columns with a unique identifier
        // if there is no grouping we will assign the same to perform then the sum
        if (groupByElement == null) {
            groupingWithoutGroupBy();
        } else {
            groupingWithGroupBy(groupByElement);
        }


        // now will we perform the sum
        if (!sumFunctions.isEmpty() ) {
            computeSum();
        } else {
            noSum();
        }

        System.out.println("SumOperator final: " + this.tuples);
    }

    /**
     * Returns the next tuple in the result set.
     * If there are no more tuples, returns null.
     */
    @Override
    public Tuple getNextTuple() {
        if (this.tuples.isEmpty()) {
            return null;
        }
        return this.tuples.remove(0);
    }

    /**
     * Resets the operator to the beginning of the result set.
     */
    @Override
    public void reset() {
        this.tuples.clear();
        this.groups.clear();
        this.child.reset();
    }

    public void groupingWithoutGroupBy() {
        List<Tuple> grouping = new ArrayList<>();
        Tuple tuple = child.getNextTuple();

        // Collect all tuples
        while (tuple != null) {
            grouping.add(tuple);
            tuple = child.getNextTuple();
        }

        // Store the single group under an arbitrary key (empty list as key)
        this.groups.put(Collections.emptyList(), grouping);
    }


    /**
     * Groups the tuples by the columns specified in the GROUP BY clause.
     */

    public void groupingWithGroupBy(GroupByElement groupByElement) {
        child.reset();
        List<Column> groupByColumns = groupByElement.getGroupByExpressionList();
        System.out.println("GroupBy Columns: " + groupByColumns);
        Tuple tuple = this.child.getNextTuple();

        while (tuple != null) {
            List<Object> groupKey = new ArrayList<>();

            System.out.println("Processing tuple: " + tuple);

            for (Column column : groupByColumns) {
                System.out.println("Processing column type: " + column.getClass());
                String columnName = column.toString();
                Object groupValue = tuple.getGroupByValue(columnName);

                if (groupValue != null) {
                    groupKey.add(groupValue);
                } else {
                    System.err.println("ERROR: GroupBy column `" + columnName + "` not found in tuple!");
                    System.err.println("Tuple contents: " + tuple);
                }
            }

            if (!groupKey.isEmpty()) {
                this.groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(tuple);
                System.out.println("Extracted Group Key: " + groupKey);
            }

            tuple = this.child.getNextTuple();
        }
    }


    /**
     * Computes the sum of the column specified in the SUM function.
     * We have three cases sum(constant), sum(column), sum(column * column) with group by
     */

    /**
     * Computes the sum of the column specified in the SUM function.
     * We have three cases: sum(constant), sum(column), sum(column * column) with group by.
     */
    public void computeSum() {
        for (Function sumFunction : this.sumFunctions) {
            Expression argument = (Expression) sumFunction.getParameters().getExpressions().get(0);
            String sumColumnName = sumFunction.toString();  // Example: "SUM(Enrolled.H)"

            Map<List<Object>, Tuple> sumResults = new HashMap<>();

            for (List<Object> key : this.groups.keySet()) {
                int sum = 0;
                Tuple representativeTuple = null;  // A reference to an existing tuple to preserve all columns

                for (Tuple tuple : this.groups.get(key)) {
                    sum += extractValueFromExpression(argument, tuple);
                    if (representativeTuple == null) {
                        representativeTuple = tuple;  // Store the first tuple in the group
                    }
                }

                if (representativeTuple != null) {
                    // ✅ Create a new tuple based on an existing one
                    Tuple newTuple = new Tuple();

                    // ✅ Copy all original columns from the representative tuple
                    for (Tuple.TupleElement element : representativeTuple.getElements()) {
                        newTuple.add(element.column, element.table, element.value);
                    }

                    // ✅ Add the computed sum as a new column
                    newTuple.add(sumColumnName, "SUM_RESULT", sum);

                    sumResults.put(key, newTuple);
                }
            }

            this.tuples.addAll(sumResults.values());
        }
    }



    public void noSum() {
        for (List<Object> key : this.groups.keySet()) {
            List<Tuple> tuples = this.groups.get(key);
            if (!tuples.isEmpty()) {  // Avoids accessing an empty list
                this.tuples.add(tuples.get(0)); // Adds the first tuple from each group
            }
        }
    }

    /**
     * Helper function to extract values from different types of expressions.
     * We have three cases: sum(constant), sum(column), sum(column * column) with group by.
     */
    private int extractValueFromExpression(Expression argument, Tuple tuple) {
        if (argument instanceof LongValue) {
            return (int) ((LongValue) argument).getValue();  // Case 1: sum(constant)
        } else if (argument instanceof Column) {
            String columnName = ((Column) argument).getColumnName();  // Case 2: sum(column)
            return tuple.getGroupByValue(columnName); // Retrieve value from tuple
        } else if (argument instanceof BinaryExpression) {
            BinaryExpression binaryExpr = (BinaryExpression) argument;
            int leftValue = extractValueFromExpression(binaryExpr.getLeftExpression(), tuple);
            int rightValue = extractValueFromExpression(binaryExpr.getRightExpression(), tuple);
            return leftValue * rightValue;  // Case 3: sum(column * column)
        }
        throw new IllegalArgumentException("Unsupported SUM expression: " + argument);
    }

}

// maybe change the gegroupby value!!




