package ed.inf.adbs.blazedb;

import java.io.*;

import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import ed.inf.adbs.blazedb.operator.Operator;
import ed.inf.adbs.blazedb.query.*;
import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import net.sf.jsqlparser.JSQLParserException;

/**
 * Lightweight in-memory database system.
 *
 * This class is the entry point for the BlazeDB system for executing queries.
 * It reads the input file, executes the query plan using an iterator model
 * and writes the result to the output file.
 *
 */
public class BlazeDB {

	/**
	 * Entry point for the BlazeDB system to set up the database and execute queries.
	 *
	 * @param args The command line arguments which consists of the database directory,
	 *             input file and output file.
	 */

	public static void main(String[] args) throws JSQLParserException, FileNotFoundException {

		// Check if the number of arguments is correct
		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}

		// Extract the arguments from the command line
		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		// Create a database catalogue
		DBCatalogue dbcatalogue = new DBCatalogue(databaseDir);

		// Create a query interpreter and a query plan
		QueryInterpreter interpreter = new QueryInterpreter(dbcatalogue);

		// Interpret the input file and get the query plan
		QueryPlan queryPlan = interpreter.interpret(inputFile);

		if (queryPlan != null) {
			// Get the root operator and execute the query
			Operator root = queryPlan.getRootOperator();
			if (root != null) {
				System.out.println("Executing query plan..." + root);
				execute(root, outputFile);
				System.out.println("Query execution completed. Results written to: " + outputFile);
			} else {
				System.err.println("Error: Query plan did not return a valid root operator.");
			}
		} else {
			System.err.println("Error: Query interpreter did not return a valid query plan.");
		}
	}

	/**
	 * Executes the provided query plan by repeatedly calling `getNextTuple()`
	 * on the root object of the operator tree. Writes the result to `outputFile`.
	 *
	 * @param root The root operator of the operator tree (assumed to be non-null).
	 * @param outputFile The name of the file where the result will be written.
	 */
	public static void execute(Operator root, String outputFile) {
		try {
			// Create a BufferedWriter for result output
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
			// Retrieve the first tuple from the operator tree
			Tuple tuple = root.getNextTuple();
			//System.out.println("current tuple: " + tuple);

			// Process all tuples produced by the root operator
			while (tuple != null) {
				//System.out.println("Result IN EXECUTE BLAZEDB: " + tuple);
				writer.write(tuple.toString()); // I HAVE CHANGED THIS
				writer.newLine();

				// Retrieve the next tuple
				tuple = root.getNextTuple();
			}
			// Close the writer
			writer.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}

