package ed.inf.adbs.blazedb;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a database tuple consisting of multiple elements.
 */
public class Tuple {
    public static class TupleElement {
        public String column;
        public String table;
        public int value;

        public TupleElement(String column, String table, int value) {
            this.column = column;
            this.table = table;
            this.value = value;
        }

        @Override
        public String toString() {
            return table + "." + column + "=" + value; // table.column=value format
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            TupleElement that = (TupleElement) obj;
            return value == that.value && Objects.equals(column, that.column) && Objects.equals(table, that.table);
        }



        //@Override
        //public int hashCode() {
        //    return Objects.hash(column, table, value);
        //}
    }


    private final List<TupleElement> elements; // List of elements in the tuple

    public Tuple() {
        this.elements = new ArrayList<>(); // Initialize the list
    }
    /**
     * Adds a column-value pair to the tuple.
     *
     * @param column Column name.
     * @param table  Table name.
     * @param value  Value (Supports int, double, string, etc.).
     */
    public void add(String column, String table, int value) {
        this.elements.add(new TupleElement(column, table, value));
    }

    public List<TupleElement> getElements() {
        return elements;
    }

    /**
     * Retrieves the value of a column from a specific table.
     *
     * @param column Column name.
     * @param table Table name.
     * @return The value if found, otherwise null.
     */
    public Integer returnValue(String column, String table) {
        for (TupleElement tupleElement : elements) {
            if (tupleElement.column.equals(column) && tupleElement.table.equals(table)) {
                return tupleElement.value;
            }
        }
        System.out.println("Warning: Column " + table + "." + column + " not found in tuple.");
        return null;
    }


    /**
     * Retrieves the value for the column used in GROUP BY.
     *
     * @param groupByColumn The GroupByElement specifying the column.
     * @return The corresponding value or throws an exception if not found.
     */
    public int getGroupByValue(String groupByColumn) {
        if (groupByColumn == null) {
            throw new IllegalArgumentException("GroupByElement is null.");
        }
        System.out.println("groupByColumn in tuple: " + groupByColumn);
        for (TupleElement element : elements) {
            String extractedColumn = groupByColumn.replaceAll(".*\\.", ""); // Removes "Student." → Keeps "B"
            if (element.column.equals(extractedColumn)) {
                System.out.println("element.value: " + element.value);
                return element.value;
            }
        }
        throw new IllegalArgumentException("GroupByElement not found in the tuple.");
    }

    /**
     * Retrieves the sum column value from the tuple.
     *
     * @param sumColumn The column to sum.
     * @return The value of the specified sum column, or throws an error if not found.
     */
    public int getSumColumnValue(String sumColumn) {
        for (TupleElement element : elements) {
            if (element.column.equals(sumColumn)) {  // Only matches "C", not "Student.C"
                return element.value;
            }
        }
        throw new IllegalArgumentException("Column " + sumColumn + " not found in the tuple.");
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < elements.size(); i++) {
            sb.append(elements.get(i).toString());
            if (i < elements.size() - 1) sb.append(", ");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Tuple tuple = (Tuple) obj;
        return Objects.equals(elements, tuple.elements);
    }

    /**
     * Merges two tuples into a new tuple.
     *
     * @param a First tuple.
     * @param b Second tuple.
     * @return A merged tuple.
     */
    public static Tuple merge(Tuple a, Tuple b) {
        Tuple t = new Tuple();
        t.elements.addAll(a.elements);
        t.elements.addAll(b.elements);
        return t;
    }

    /**
     * Adds a derived column as a copy of an existing column.
     * This is useful when performing operations like SUM(columnA) but keeping columnA.
     *
     * @param originalColumn The original column name.
     * @param originalTable  The original table name.
     * @param newColumn      The new column name to store the copied value.
     */
    public void addDerivedColumn(String originalColumn, String originalTable, String newColumn) {
        Integer value = returnValue(originalColumn, originalTable);
        if (value != null) {
            this.add(newColumn, originalTable, value);
        } else {
            System.err.println("ERROR: Cannot create derived column. Original column " + originalTable + "." + originalColumn + " not found.");
        }
    }


    public String printTuple() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < elements.size(); i++) {
            sb.append(elements.get(i).toString());
            if (i < elements.size() - 1) sb.append(", ");
        }
        return sb.toString();
    }
}

