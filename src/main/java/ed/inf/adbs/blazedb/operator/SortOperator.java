package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.ArrayList;
import java.util.List;

/**
 * SortOperator is an operator that sorts the tuples from its child operator
 * based on the order by clause.
 */

public class SortOperator extends Operator {
    private Operator child;
    private List<Tuple> tuples;
    private final List<String> columnList;
    private int currentTupleIndex = 0;

    /**
     * Constructor for SortOperator
     * @param child - child operator
     * @param orderByElements - list of order by elements
     */

    public SortOperator(Operator child, List<OrderByElement> orderByElements) {
        this.child = child;
        this.columnList = extractColumnNames(orderByElements);
        this.tuples = new ArrayList<>();

        // we are assuming that we will do everything in memory
        // need to load all the tuples from the child operator
        Tuple tuple = child.getNextTuple();
        while (tuple != null) {
            tuples.add(tuple);
            tuple = child.getNextTuple();
        }
        //sort them : ascending order always
        sortTupleList();
    }

    /**
     * Get the next tuple
     * @return the next tuple
     */
    @Override
    public Tuple getNextTuple() {
        if (currentTupleIndex >= tuples.size()) {
            return null;
        }
        return tuples.get(currentTupleIndex++);
    }

    /**
     * Reset the operator
     */
    @Override
    public void reset() {
        currentTupleIndex = 0;
    }

    /**
     * Sorts the tuple list based on the order defined in columnList.
     * This comparator iterates through multiple columns in priority order
     * and sorts tuples accordingly.
     *
     * - Uses Integer.compare() to compare values of the columns.
     */

    private void sortTupleList() {
        tuples.sort((t1, t2) -> {
            for (String fullColumn : columnList) {  // Iterate through all sorting columns
                // Extract table name and column name
                String[] parts = fullColumn.split("\\.");
                if (parts.length != 2) {
                    System.err.println("Error: Malformed column name " + fullColumn);
                    continue;
                }

                String tableName = parts[0];  // Extract table name
                String columnName = parts[1]; // Extract column name

                // Retrieve values correctly from the tuples
                Integer value1 = t1.returnValue(columnName, tableName);
                Integer value2 = t2.returnValue(columnName, tableName);

                //System.out.println("Debugging :Sorting by: " + fullColumn + " -> " + value1 + " vs " + value2);

                // avoid null pointer exception
                if (value1 == null || value2 == null) {
                    System.err.println("Error: Missing value for column " + fullColumn);
                    continue;
                }

                // Compare the values
                int comparison = Integer.compare(value1, value2); // this returns -1, 0, 1
                if (comparison != 0) {
                    return comparison; // Stop at the first nonzero comparison
                }
            }
            return 0; // If all sorting columns are equal, maintain original order (stable sort)
        });
    }



    /**
     * Extracts the column names from the order by elements
     * @param orderByElements - list of order by elements
     * @return list of column names
     */
    private List<String> extractColumnNames(List<OrderByElement> orderByElements) {
        List<String> columnList = new ArrayList<>();
        if (orderByElements == null) {
            return columnList;
        }

        // Extract in the correct order : this is the priority order if multiple columns are present
        for (int i = 0; i < orderByElements.size(); i++) {
            Expression expr = orderByElements.get(i).getExpression();
            if (expr instanceof Column) {  // Ensure it's a column reference
                Column column = (Column) expr;
                String tableName = (column.getTable() != null && column.getTable().getName() != null)
                        ? column.getTable().getName()
                        : "Student"; // Default to Student if table name is missing

                String fullyQualifiedColumn = tableName + "." + column.getColumnName();
                columnList.add(fullyQualifiedColumn);
                //System.out.println("Debugging Extracted Sorting Column: " + fullyQualifiedColumn);
            }
        }

        return columnList;
    }

    /**
     * Returns the string representation of the operator
     * @return string representation
     */
    public String toString() {
        return "SortOperator (columns: " + columnList + ")";
    }

}