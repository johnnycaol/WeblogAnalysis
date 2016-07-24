import static org.junit.Assert.*;
import org.junit.Test;
import java.io.IOException;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;

public class PigUnitTest {
	private PigTest test;
	private static final String SESSIONIZE_SCRIPT = "src/test/resources/pig/sessionize.pig";
	private static final String AVG_SESSIONI_LENGTH_SCRIPT = "src/test/resources/pig/avg_session_length.pig";
	private static final String AVG_URL_COUNT_PER_SESSION_SCRIPT = "src/test/resources/pig/avg_url_count_per_session.pig";
	private static final String MOST_ENGAGED_USER_IPS_SCRIPT = "src/test/resources/pig/most_engaged_user_IPs.pig";
	private static final String TEST_DATA = "src/test/resources/data/test_data.log";

	// Run pig script in command line:
	// REGISTER piggybank and datafu jar in all pig files
	// pig -x local -f sessionize.pig -param input='../data/test_data.log'
	@Test
	public void testSessionize() throws ParseException, IOException {
		String[] args = { "input=" + TEST_DATA, };

		// Construct an instance of PigTest that will use the script
		test = new PigTest(SESSIONIZE_SCRIPT, args);

		// Run the pig script
		test.runScript();

		// Initialize an ArrayList to store all generated session ids
		List<String> sessionIds = new ArrayList<String>();

		// The "sessionized_logs" tells PigUnit what alias to get from.
		for (Tuple t : this.getLinesForAlias(test, "sessionized_logs")) {
			String sessionId = (String) t.get(19);
			sessionIds.add(sessionId);
		}

		// Run the test and check that the output matches our expectation.
		assertEquals(47, sessionIds.size());

		// First session assertions for 192.168.0.1
		Integer session1Index = 0;
		Integer session1Length = 17;//make sure the session id from index 0 to 16 are all the same
		String session1 = sessionIds.get(session1Index);
		for (int i = session1Index + 1; i < session1Index + session1Length; i++) {
			assertEquals(session1, sessionIds.get(i));
		}

		// Second session assertions for 192.168.0.1
		Integer session2Index = 17;
		Integer session2Length = 3;
		String session2 = sessionIds.get(session2Index);
		assertNotEquals(session1, session2);
		for (int i = session2Index + 1; i < session2Index + session2Length; i++) {
			assertEquals(session2, sessionIds.get(i));
		}

		// Third session assertions for 192.168.0.1
		Integer session3Index = 20;
		Integer session3Length = 3;
		String session3 = sessionIds.get(session3Index);
		assertNotEquals(session2, session3);
		for (int i = session3Index + 1; i < session3Index + session3Length; i++) {
			assertEquals(session3, sessionIds.get(i));
		}

		// First session assertions for 192.168.0.2
		Integer session4Index = 23;
		Integer session4Length = 18;
		String session4 = sessionIds.get(session4Index);
		assertNotEquals(session3, session4);
		for (int i = session4Index + 1; i < session4Index + session4Length; i++) {
			assertEquals(session4, sessionIds.get(i));
		}

		// First session assertions for 192.168.0.3
		Integer session5Index = 41;
		Integer session5Length = 3;
		String session5 = sessionIds.get(session5Index);
		assertNotEquals(session4, session5);
		for (int i = session5Index + 1; i < session5Index + session5Length; i++) {
			assertEquals(session5, sessionIds.get(i));
		}

		// First session assertions for 192.168.0.4
		Integer session6Index = 44;
		Integer session6Length = 3;
		String session6 = sessionIds.get(session6Index);
		assertNotEquals(session5, session6);
		for (int i = session6Index + 1; i < session6Index + session6Length; i++) {
			assertEquals(session6, sessionIds.get(i));
		}
	}

	@Test
	public void testAvgSessionLength() throws ParseException, IOException {
		String[] args = { "input=" + TEST_DATA, };

		// Construct an instance of PigTest that will use the script
		test = new PigTest(AVG_SESSIONI_LENGTH_SCRIPT, args);

		// Initialize the expected output
		String[] output = { "(1968.67)", };

		// Run the pig script and check the results
		test.assertOutput("avg_session_length", output);
	}

	@Test
	public void testAvgUrlCountPerSession() throws ParseException, IOException {
		String[] args = { "input=" + TEST_DATA, };

		// Construct an instance of PigTest that will use the script
		test = new PigTest(AVG_URL_COUNT_PER_SESSION_SCRIPT, args);

		// Initialize the expected output
		String[] output = { "(5.33)", };

		// Run the pig script and check the results
		test.assertOutput("avg_unique_url_count", output);
	}

	@Test
	public void testMostEngagedUserIps() throws ParseException, IOException {
		String[] args = { "input=" + TEST_DATA, };

		// Construct an instance of PigTest that will use the script
		test = new PigTest(MOST_ENGAGED_USER_IPS_SCRIPT, args);

		// Initialize the expected output
		String[] output = { "(192.168.0.2)", };

		// Run the pig script and check the results
		test.assertOutput("most_engaged_users", output);
	}

	/**
	 * Get the lines of the output alias
	 */
	private List<Tuple> getLinesForAlias(PigTest test, String alias) throws IOException, ParseException {
		Iterator<Tuple> tuplesIterator = test.getAlias(alias);
		List<Tuple> tuples = new ArrayList<Tuple>();

		while (tuplesIterator.hasNext()) {
			Tuple tuple = tuplesIterator.next();
			tuples.add(tuple);
		}
		return tuples;
	}
}
