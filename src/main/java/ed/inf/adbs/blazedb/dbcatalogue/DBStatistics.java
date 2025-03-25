package ed.inf.adbs.blazedb.dbcatalogue;

import ed.inf.adbs.blazedb.operator.ScanOperator;
import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import java.util.*;
import net.sf.jsqlparser.statement.select.FromItem;
import ed.inf.adbs.blazedb.Tuple;
import java.util.stream.Collectors;

/**
 * This class is used to store statistics about the database.
 * It can be initiated once with a ScanOperator for each of the tables.
 */
public class DBStatistics {
    private final DBCatalogue dbCatalogue;
    private ScanOperator scanOperator;
    private final Map<String, Map<String, List<Integer>>> tableStatistics; // Table -> Column -> [min, max, distinctCount, ntuples]

    public DBStatistics(DBCatalogue dbCatalogue) {
        this.dbCatalogue = dbCatalogue;
        this.tableStatistics = new HashMap<>();
    }

    /**
     * Computes statistics (min, max, distinct count) per column for a table.
     */
    public void computeStatistics(FromItem table) {
        scanOperator = new ScanOperator(table, dbCatalogue);
        List<Tuple> tuples = new ArrayList<>();

        // Collect all tuples from the ScanOperator
        Tuple tuple;
        while ((tuple = scanOperator.getNextTuple()) != null) {
            tuples.add(tuple);
        }

        if (tuples.isEmpty()) {
            return; // No statistics to compute for an empty table.
        }


        // Get column names from the table
        List<String> columnNames = dbCatalogue.getTableColumns(table.toString());
        System.out.println("columnnames"+ columnNames);

        // Map to store per-column statistics
        Map<String, List<Integer>> columnStats = new HashMap<>();

        for (String column : columnNames) {
            // Extract column values and ensure they are integers
            List<Integer> columnValues = tuples.stream()
                    .map(t -> (Integer) t.returnValue(column,table.toString()))  // Ensure correct method call for fetching values
                    .collect(Collectors.toList());

            if (columnValues.isEmpty()) {
                continue; // Skip empty columns
            }

            // Compute min, max, and distinct count using Java Collections API
            int minValue = Collections.min(columnValues);
            int maxValue = Collections.max(columnValues);
            int distinctCount = (int) columnValues.stream().distinct().count();
            int nTuples = columnValues.size();

            // Store computed statistics as a list [min, max, distinctCount, nTuples]
            columnStats.put(column, Arrays.asList(minValue, maxValue, distinctCount, nTuples));
        }

        // Store table statistics
        tableStatistics.put(table.toString(), columnStats);
    }

    /**
     * Retrieves statistics for a specific table and column.
     */
    public List<Integer> getColumnStatistics(String tableName, String columnName) {
        return tableStatistics.getOrDefault(tableName, Collections.emptyMap()).get(columnName);
    }

}
