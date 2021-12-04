package server.faulttolerance;

import client.MyDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;
import server.AVDBReplicatedServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;

	/**
	 * Values needed to establish a connection with Cassandra
 	 */
	final private Cluster cluster;
	final protected Session session;

	/**
	 * Specified keyspace for the current server
	 */
	final private String keyspace;

	/**
	 * Default table name required for the checkpoint and restore functions
	 */
	protected final static String DEFAULT_TABLE_NAME = "grade";


	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {

		// Assigned the keyspace to this.keyspace using the first argument in args
		this.keyspace = args[0];
		// Debugging: System.out.println("My key space is" + args[0]);

		// Establish connection to the Cassandra host
		session = (cluster = Cluster.builder().addContactPoint("127.0.0.1")
				.build()).connect(this.keyspace);
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		return this.execute(request);
	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		// This method first casts the given request to RequestPacket
		// We then extract the query to be executed on the database
		// The query is executed using session.execute() on the database
		// We do not account for consistency or fault tolerance as we expect
		// GigaPaxos to handle this

		if (request instanceof RequestPacket) {
			String clientRequest = ((RequestPacket) request).requestValue;
			ResultSet results = session.execute(clientRequest);
			StringBuilder response = new StringBuilder();
			for (Row row : results) {
				response.append(row.toString());
			}
		}
		else
			System.err.println("Unknown packet type: " + request.getSummary());
		return true;
	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		// This method checkpoints and stores the current state of the database
		// We get all the existing values in the database and append it to the response
		// string using delimiters

		String checkPointCommand = "select * from " + keyspace + "." + DEFAULT_TABLE_NAME + ";";
		ResultSet results = session.execute(checkPointCommand);
		StringBuilder response = new StringBuilder();
		for (Row row : results) {
			response.append(row.getInt("key"));
			for (int event : row.getList("events", int.class)) {
				response.append(":").append(event);
			}
			response.append("|");
		}
		return response.toString();
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */
	@Override
	public boolean restore(String s, String s1) {
		// This method restores the state of the database from the previous checkpoints
		// We use the delimiters to get the required values and insert the values into the database

//		try {
//			String createTableCommand = "create table if not exists " + keyspace + "." + DEFAULT_TABLE_NAME + " (id " +
//					"" + "" + "" + "" + "" + "" + "" + "" + "" + "int," + " " +
//					"events" + "" + "" + " " + "list<int>, " + "primary " + "" +
//					"key" + " " + "" + "" + "" + "(id)" + ");";
//			session.execute(createTableCommand);
//			String[] rows = s.split("\\|");
//
//			for (String row: rows) {
//				String[] values = row.split(":");
//				int key = Integer.parseInt(values[0]);
//				String insertEmptyCommand = "insert into " + DEFAULT_TABLE_NAME + " (id, events) values (" + key + ", "
//						+ "[]);";
//				session.execute(insertEmptyCommand);
//
//				for (int i = 1; i < values.length; i++) {
//					String updateCommand = "update " + DEFAULT_TABLE_NAME + " SET events=events+[" + values[i] + "] " +
//							"where id=" + key + ";";
//
//					session.execute(updateCommand);
//				}
//			}
//		} catch (NumberFormatException e) {
//			e.printStackTrace();
//		}
		return true;

	}


	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
	 * need to be implemented because the assignment Grader will only use
	 * {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}

}