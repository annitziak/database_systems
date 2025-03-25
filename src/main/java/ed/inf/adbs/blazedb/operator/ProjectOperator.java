package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.Tuple.TupleElement;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;

public class ProjectOperator extends Operator {

    private final Operator child;
    private final List<SelectItem<?>> selectColumns;

    /**
     * Constructs a ProjectOperator with the specified child operator and selected columns.
     *
     * @param child         The child operator producing tuples.
     * @param selectColumns List of column names to project.
     */
    public ProjectOperator(Operator child, List<SelectItem<?>> selectColumns) {
        this.child = child;
        this.selectColumns = selectColumns;
    }

    /**
     * Processes a tuple by selecting only the required columns.
     *
     * @param initialTuple The input tuple from the child operator.
     * @return A new tuple with only the selected columns.
     */
    private Tuple processTuple(Tuple initialTuple) {
        Tuple outputTuple = new Tuple();

        // If SELECT * is used (i.e., all columns are selected), return the full tuple
        if (selectColumns.isEmpty() || selectColumns.get(0).toString().equals("*")) {
            return initialTuple;
        }
        // Iterate over the selected columns, we have to handle two cases:
        // 1. Standard column selection (e.g., "Student.C") -> class Column
        // 2. Aggregation functions (e.g., "SUM(Student.C)") -> class Function
        for (SelectItem<?> selectItem : selectColumns) {
            Expression expr = selectItem.getExpression();

            boolean found = false;  // keep track if the column or function was found in the tuple

            // Case 1: Column projection (Standard column selection)
            if (expr instanceof Column) {
                String columnName = ((Column) expr).getColumnName();

                // make a new output tuple with the selected column
                for (TupleElement element : initialTuple.getElements()) {
                    if (element.column.equals(columnName)) {
                        outputTuple.add(element.column, element.table, element.value);
                        found = true; // mark the column as found
                        break; // no need to continue searching
                    }
                }
            }
            // Aggregation functions (SUM)
            else if (expr instanceof Function) {
                // since we only expect SUM, we can directly check for the function name
                String functionName = ((Function) expr).getName().toUpperCase();
                if (functionName.equals("SUM")) {
                    // Retrieve the correct SUM column name
                    String sumColumnName = expr.toString();  // Example: "SUM(Student.C)" or "SUM(1)"

                    for (TupleElement element : initialTuple.getElements()) {
                        if (element.column.equals(sumColumnName)) {
                            outputTuple.add(sumColumnName, element.table, element.value); // add the SUM column
                            found = true; // mark the function as found
                            break; // no need to continue searching
                        }
                    }
                }
            }

            if (!found) {
                System.err.println("Projection Error: Column or function `" + expr + "` not found in tuple.");
            }
        }

        return outputTuple;
    }

    /**
     * Retrieves the next tuple, applies projection if necessary.
     *
     * @return The next tuple after projection, or null if no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple nextTuple = child.getNextTuple();
        return (nextTuple != null) ? processTuple(nextTuple) : null;
    }

    /**
     * Resets the operator to the initial state.
     */
    @Override
    public void reset() {
        child.reset();
    }

    public String toString() {
        return "ProjectOperator(columns=" + selectColumns + ")";
    }
}
