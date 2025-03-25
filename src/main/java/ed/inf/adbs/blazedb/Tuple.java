package ed.inf.adbs.blazedb;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


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

        //@Override
        //public boolean equals(Object obj) {
        //     if (this == obj) return true;
        //     if (obj == null || getClass() != obj.getClass()) return false;
        //     TupleElement that = (TupleElement) obj;
        //     return value == that.value && Objects.equals(column, that.column) && Objects.equals(table, that.table);
        // }


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
     * @param table  Table name.
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
     * @param Column The Column.
     * @return The corresponding value or throws an exception if not found.
     */
    public int getColumnValue(String Column) {
        if (Column == null) {
            throw new IllegalArgumentException("GroupByElement is null.");
        }
        for (TupleElement element : elements) {
            String extractedColumn = Column.replaceAll(".*\\.", ""); // Removes "Student." → Keeps "B"
            if (element.column.equals(extractedColumn)) {
                return element.value;
            }
        }
        throw new IllegalArgumentException("Column not found in tuple: " + Column);
    }

    // in case you want the whole tuple in the form Student.B=5, Student.C=10 ..
    // this is very useful for debugging
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < elements.size(); i++) {
            sb.append(elements.get(i).toString());
            if (i < elements.size() - 1) sb.append(", ");
        }
        return sb.toString();
    }


    /**
     * Merges two tuples into a new tuple. This is used for the Expression Visitor.
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
     * Prints only the values of the tuple. This is useful for final results.
     *
     * @return A string of values separated by commas.
     */
    public String printValuesOnly() {
        return elements.stream()
                .map(e -> String.valueOf(e.value))
                .collect(Collectors.joining(", "));
    }

    /**
     * Adds a derived column as a copy of an existing column. Not used here
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

}

