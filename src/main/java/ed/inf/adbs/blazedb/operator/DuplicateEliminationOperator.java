package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

import java.util.*;


public class DuplicateEliminationOperator extends Operator {
    private final Operator child;
    private final String uniqueColumn;
    private List<Tuple> sortedTuples;
    private int currentIndex = 0;


    /**
     * Constructor for DuplicateEliminationOperator
     * @param child The child operator
     * @param uniqueColumn The column(s) to remove duplicates on
     */

    // this here needs to be fixed as multiple columns can be passed
    public DuplicateEliminationOperator(Operator child, String uniqueColumn) {
        this.child = child;
        this.uniqueColumn = uniqueColumn;
        this.sortedTuples = new ArrayList<>();

        // Load all tuples from child operator, sort them and remove duplicates
        loadTuples();
        sortTupleList();
        removeDuplicates();
    }

    /** Load all tuples from child operator and save them in sortedTuples list.
    // This is a blocking operation so all of them need to be loaded before sorting.
     */
    private void loadTuples() {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            sortedTuples.add(tuple);
        }
    }

    /** Sort the list of tuples based on the unique column.
     * We only expect integer values in the unique column.
     */
    private void sortTupleList() {
        // for each pair of tuples, compare the unique column values
        sortedTuples.sort((t1, t2) -> {
            String[] parts = uniqueColumn.split("\\.");
            if (parts.length != 2) {
                System.err.println("Error: Malformed column name " + uniqueColumn);
                return 0;
            }

            // Extract the table name and column name
            String tableName = parts[0];
            String columnName = parts[1];

            Integer value1 = t1.returnValue(columnName, tableName); // Get the value of the column
            Integer value2 = t2.returnValue(columnName, tableName); // Get the value of the column

            if (value1 == null || value2 == null) {
                return 0; // Don't reorder if values are missing
            }

            // Perform Integer comparison
            return Integer.compare(value1, value2);
        });
    }

    /** Remove duplicates from the sortedTuples list based on the unique column.
     * This is a blocking operation.
     */
    private void removeDuplicates() {
        if (sortedTuples.isEmpty()) return;


        // initialize a list for the unique tuples and a set to keep track of the values we have seen
        // if we have seen them, we don't add them to the uniqueTuples list, otherwise we do.
        List<Tuple> uniqueTuples = new ArrayList<>();
        Set<Integer> seenValues = new HashSet<>();

        // Extract the table name and column name
        String[] parts = uniqueColumn.split("\\.");
        if (parts.length != 2) {
            System.err.println("Error: Malformed column name " + uniqueColumn);
            return;
        }
        String tableName = parts[0];
        String columnName = parts[1];

        // iterate through the sorted tuples and add the unique ones to the uniqueTuples list
        for (Tuple tuple : sortedTuples) {
            Integer columnValue = tuple.returnValue(columnName, tableName);
            if (columnValue != null && !seenValues.contains(columnValue)) {
                uniqueTuples.add(tuple);
                seenValues.add(columnValue);
            }
        }
        // update the sortedTuples list with the unique tuples
        sortedTuples = uniqueTuples;
    }

    /**
     * Get the next tuple from the operator
     * @return The next tuple
     */
    @Override
    public Tuple getNextTuple() {
        if (currentIndex < sortedTuples.size()) {
            return sortedTuples.get(currentIndex++);
        }
        return null; // No more tuples
    }

    /**
     * Reset the operator
     */
    @Override
    public void reset() {
        currentIndex = 0; // Reset iterator by setting index to 0.
    }

    /**
     * Get the operator
     * @return The operator.
     */
    public String toString() {
        return "DuplicateEliminationOperator";
    }
}
