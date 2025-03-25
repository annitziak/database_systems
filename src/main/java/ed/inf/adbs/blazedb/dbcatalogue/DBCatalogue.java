package ed.inf.adbs.blazedb.dbcatalogue;

import java.io.*;
import java.util.*;

public class DBCatalogue {
    private static DBCatalogue instance;
    private final String databaseDir;
    private final HashMap<String, List<String>> tables;

    /**
     * Private constructor to enforce Singleton pattern for the DBCatalogue.
     * Loads the schema from the schema.txt file.
     * @param directory The directory where the database is located.
     *                  The schema.txt file should be located in this directory.
     *                  The data directory should be located in this directory under "db/data"
     *                  that contains the data files for the tables. named as table_name.csv.
     */
    public DBCatalogue(String directory) {
        this.databaseDir = directory; // in our example this is "samples"
        this.tables = new HashMap<>();
        loadSchema();
    }

    /**
     * Returns the singleton instance of DBCatalogue
     */
    public static DBCatalogue getInstance(String directory) {
        // Initialize the instance if it is not already initialized
        if (instance == null) {
            instance = new DBCatalogue(directory);
        }
        return instance;
    }

    /**
     * Loads table schema from the schema.txt file located in the database directory.
     * The schema file should have the following format:
     * table_name column1 column2 column3 ...
     *
     */
    private void loadSchema() {
        // Load schema from the schema.txt file
        // Use File.separator to make the code platform independent
        String schemaFile = databaseDir + File.separator + "schema.txt";

        // initialize a reader to read the schema file line by line
        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.trim().split("\\s+");  // Correctly split by spaces

                // Parts should have at least 2 elements: table name and at least one column
                // Otherwise, the line is invalid
                if (parts.length < 2) {
                    System.err.println("Invalid schema line: " + line);
                    continue;
                }
                // First part is the table name
                String tableName = parts[0].trim();

                // Initialize and collect the list of columns for the table following the table name
                List<String> columns = new ArrayList<>();
                for (int i = 1; i < parts.length; i++) {
                    columns.add(parts[i].trim());
                }
                // the format of tables is a map of table name to list of columns
                // example : table1 -> [column1, column2, column3]
                tables.put(tableName, columns);

            }
        } catch (IOException e) {
            System.err.println("Error loading schema from: " + schemaFile);
            e.printStackTrace();
        }
    }

    /**
     * Returns the directory of the table data file if needed.
     * The data file should be located in the db directory under the name table_name.csv.
     * @param table The name of the table.
     */
    public String getDirectory(String table) {
        System.out.println(databaseDir + File.separator +
                "data" + File.separator + table + ".csv");
        return databaseDir + File.separator +
                "data" + File.separator + table + ".csv";
    }

    /**
     * Retrieves all columns for a given table.
     * This will be helpful if we need to get the columns of a table (e.g. for Early projections)
     * @param table The name of the table.
     */
    public List<String> getTableColumns(String table) {
        return tables.getOrDefault(table, Collections.emptyList());
    }

    /**
     * Checks if a table exists in the schema, i.e., if the table is loaded and exists
     * in the DBCatalogue. This is important to check before executing a query on a table.
     * @param table The name of the table.
     */
    public boolean tableExists(String table) {
        return tables.containsKey(table);
    }

    /**
     * Retrieves all tables in the schema.
     */
    public List<String> getAllTables() {
        return new ArrayList<>(tables.keySet()); }

    /**
     * Prints all loaded tables and their columns.
     * Used for debugging purposes.
     */
    public void printSchema() {
        System.out.println("Database Schema:");
        for (Map.Entry<String, List<String>> entry : tables.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
    }
}
