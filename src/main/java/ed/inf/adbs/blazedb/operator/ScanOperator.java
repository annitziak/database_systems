package ed.inf.adbs.blazedb.operator;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.List;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import ed.inf.adbs.blazedb.operator.Operator;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.schema.Table;

/**
 * ScanOperator performs a table scan by reading tuples from a file.
 * It validates the input table and ensures the file exists before processing.
 */
public class ScanOperator extends Operator {
    private final DBCatalogue dbCatalogue;
    private final String tableName;
    private BufferedReader reader;

    /**
     * Constructs a ScanOperator for reading a table's data.
     *
     * @param fromItem    The table reference in the SQL query.
     * @param dbCatalogue The database catalog containing table metadata.
     * @throws IllegalArgumentException if the table is invalid or does not exist.
     * @throws RuntimeException if the table file cannot be opened.
     */
    public ScanOperator(FromItem fromItem, DBCatalogue dbCatalogue) {
        this.dbCatalogue = dbCatalogue;

        // Validate that fromItem is a Table and extract its name
        if (!(fromItem instanceof Table)) {
            throw new IllegalArgumentException("Invalid FromItem: Expected a table, but received " + fromItem);
        }
        this.tableName = ((Table) fromItem).getName();

        // Validate if the table exists in the database catalogue
        // This is good practice to ensure the table exists before proceeding.
        if (!dbCatalogue.tableExists(tableName)) {
            throw new IllegalArgumentException("Table '" + tableName + "' does not exist in the database.");
        }

        // Open the file reader for the table file
        try {
            this.reader = openFileReader(tableName);
        } catch (IOException e) {
            throw new RuntimeException("Error opening file for table: " + tableName, e);
        }
    }

    /**
     * Opens a BufferedReader for reading a table file.
     *
     * @param tableName The name of the table.
     * @return A BufferedReader to read the table file.
     * @throws IOException If the file does not exist or cannot be opened.
     */
    public BufferedReader openFileReader(String tableName) throws IOException {
        String path = dbCatalogue.getDirectory(tableName);

        if (path == null) {
            throw new IOException("Table not found in schema: " + tableName);
        }
        return Files.newBufferedReader(Paths.get(path));
    }


@Override
    public Tuple getNextTuple() {
        //will read line by line and get the comma separated values and store them in a list
        try {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }

            String[] parts = line.split(",");
            //make them integers
            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].trim(); //trim the whitespaces
            }
            //need to get all the columns from the db catalogue
            List<String> columns = dbCatalogue.getTableColumns(tableName);
            if (columns.size() != parts.length) {
                throw new RuntimeException("Schema mismatch: expected " + columns.size() + " columns but found " + parts.length);
            }

            // Create a new tuple instance by adding col, table, value
            Tuple tuple = new Tuple();
            for (int i = 0; i < columns.size(); i++) {
                try {
                    int value = Integer.parseInt(parts[i]);
                    tuple.add(columns.get(i), tableName, value);
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid integer value in table " + tableName + " at column " + columns.get(i) + ": " + parts[i], e);
                }
            }
            //System.out.println("Debugging: Returning tuple [SCAN]: " + tuple);
            return tuple;

        } catch (IOException e) {
            throw new RuntimeException("Error reading next tuple from table: " + tableName, e);
        }
    }
    /**
     * Closes the file reader when the operator is closed.
     */

    @Override
    public void reset() {
        // close the current reader and open a new one
        try {
            reader.close();
            // Reopen the file reader
            reader = openFileReader(tableName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to reset ScanOperator for table: " + tableName, e);
        }
    }
}

