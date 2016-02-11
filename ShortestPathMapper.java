package pkg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ShortestPathMapper extends Mapper<Object, Text, Text, Text> {
	private String nodeID;
	private String nodeDetails;

	// Declaring a counter to track the iteration condition
	public static enum PROGRAM_COUNTERS {
		MORE_ITERATIONS
	};

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Mapper start");
		// Split the line to get nodeID and other information as strings
		String nodeTemp[] = value.toString().split("\\t");
		nodeID = nodeTemp[0];
		nodeDetails = nodeTemp[1];
		// Creating a Node object and passing nodeID and nodeDetails as input
		// Individual components of nodes like adjacency list, distance from
		// source, color, source etc are generated by the Node class. Those
		// variables can be accessed used getters and setters
		Node node = new Node(nodeID, nodeDetails);
		if (node.getNodeColor().equals("BLACK")
				|| node.getNodeColor().equals("WHITE")) {
			context.write(new Text(nodeID), new Text(nodeDetails));
		} else {
			StringBuffer sb = new StringBuffer();
			// Return the same line with color changed to black
			sb.append(node.getAdjListString() + "|"
					+ node.getDistanceFromSource() + "|" + "BLACK" + "|"
					+ node.getSourceNode());
			context.write(new Text(nodeID), new Text(sb.toString()));
			for (int i = 0; i < node.getAdjacencyList().size(); i++) {
				// Reset the string buffer every time the loop iterates
				sb.delete(0, sb.length());
				// Split the line into multiple lines and color them gray
				sb.append(null + "|" + (node.getDistanceFromSource() + 1) + "|"
						+ "GRAY" + "|" + nodeID);
				System.out.println("Mapper end");
				context.getCounter(PROGRAM_COUNTERS.MORE_ITERATIONS)
						.setValue(0);
				context.write(new Text(node.getAdjacencyList().get(i)),
						new Text(sb.toString()));
			}
		}
	}
}