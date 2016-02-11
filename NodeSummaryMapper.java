package pkg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NodeSummaryMapper extends Mapper<Object, Text, Text, Text> {

	private String nodeID;
	private String nodeDetails;
	private int numberOfLinks;

	/*
	 * This is the mapper class for first job in the sequence. Mapper will read
	 * each line from the input files, splits it into nodeID and nodDetails and
	 * writes it to the reducer as a Key,Value pair
	 */
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Node Summary Mapper start");
		// Split the string to get node name and other details
		String nodeTemp[] = value.toString().split("\\t");
		nodeID = nodeTemp[0];
		nodeDetails = nodeTemp[1];
		Node node = new Node(nodeID, nodeDetails);
		numberOfLinks = node.getAdjacencyList().size();
		// Write the output to reducer
		context.write(new Text(nodeID), new Text(node.getAdjListString() + "\t"
				+ numberOfLinks));

	}
}