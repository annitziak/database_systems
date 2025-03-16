package ed.inf.adbs.blazedb;

import static org.junit.Assert.assertTrue;

import net.sf.jsqlparser.JSQLParserException;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Unit tests for BlazeDB.
 */
public class BlazeDBTest {
	
	/**z
	 * Rigorous Test :-)
	 */
	@Test
	public void shouldAnswerWithTrue() {
		assertTrue(true);
	}


	// database_dir input_file output_file
	@Test
	public void query1Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query1.sql", base + "test_output" + File.separator + "output1.txt"});
	}

	@Test
	public void query2Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query2.sql", base + "test_output" + File.separator + "output2.txt"});
	}

	@Test
	public void query3Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query3.sql", base + "test_output" + File.separator + "output3.txt"});
	}

	@Test
	public void query4Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query4.sql", base + "test_output" + File.separator + "output4.txt"});
	}

	@Test
	// SELECT * FROM Student, Enrolled WHERE Student.A = Enrolled.A;
	public void query5Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query5.sql", base + "test_output" + File.separator + "output5.txt"});
	}

	@Test
	// SELECT * FROM Student, Course WHERE Student.C < Course.E;
	public void query6Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query6.sql", base + "test_output" + File.separator + "output6.txt"});
	}

	@Test
	public void query7Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query7.sql", base + "test_output" + File.separator + "output7.txt"});
	}

	@Test
	public void query8Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query8.sql", base + "test_output" + File.separator + "output8.txt"});
	}

	@Test
	// still to fix
	// SELECT Enrolled.E, SUM(Enrolled.H * Enrolled.H) FROM Enrolled GROUP BY Enrolled.E;
	public void query9Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query9.sql", base + "test_output" + File.separator + "output9.txt"});
	}

	@Test
	// SELECT SUM(1) FROM Student GROUP BY Student.B;
	public void query10Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query10.sql", base + "test_output" + File.separator + "output10.txt"});
	}

	@Test
	//SELECT Student.B, Student.C FROM Student, Enrolled WHERE Student.A = Enrolled.A GROUP BY Student.B, Student.C ORDER BY Student.C, Student.B;
	public void query11Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query11.sql", base + "test_output" + File.separator + "output11.txt"});
	}

	@Test
	// still to fix
	// SELECT SUM(1), SUM(Student.A) FROM Student, Enrolled;
	public void query12Test() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "input" + File.separator + "query12.sql", base + "test_output" + File.separator + "output12.txt"});
	}

	@Test
	public void query1TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query1.sql", base + "test_output_extra" + File.separator + "output1.txt"});
	}

	@Test
	public void query2TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query2.sql", base + "test_output_extra" + File.separator + "output2.txt"});
	}
	@Test
	public void query3TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query3.sql", base + "test_output_extra" + File.separator + "output3.txt"});
	}

	@Test
	public void query4TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query4.sql", base + "test_output_extra" + File.separator + "output4.txt"});
	}

	@Test
	public void query5TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query5.sql", base + "test_output_extra" + File.separator + "output5.txt"});
	}

	@Test
	public void query6TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query6.sql", base + "test_output_extra" + File.separator + "output6.txt"});
	}

	@Test
	public void query7TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query7.sql", base + "test_output_extra" + File.separator + "output7.txt"});
	}

	@Test
	public void query8TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query8.sql", base + "test_output_extra" + File.separator + "output8.txt"});
	}

	@Test
	public void query9TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query9.sql", base + "test_output_extra" + File.separator + "output9.txt"});
	}

	@Test
	public void query10TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query10.sql", base + "test_output_extra" + File.separator + "output10.txt"});
	}

	@Test
	public void query11TestExtra() throws JSQLParserException, FileNotFoundException {
		BlazeDB blazeDB = new BlazeDB();
		String base = "samples" + File.separator;
		BlazeDB.main(new String[] {base+"db", base + "extra_input" + File.separator + "query11.sql", base + "test_output_extra" + File.separator + "output11.txt"});
	}


}

// one function per query and input output