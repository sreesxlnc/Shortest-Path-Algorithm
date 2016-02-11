package pkg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pkg.ShortestPathMapper.PROGRAM_COUNTERS;

public class ShortestPathReducer extends Reducer<Text, Text, Text, Text> {
	/*
	 * Reducer combines the output from Mapper, consolidates all the information
	 * about a particular node and writes it to the output
	 */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int thisDistance = 0;
		String thisNodeColor = "null";
		String thisAdjList = "null";
		String thisSourceNode = "null";
		boolean receivedBlack = false;
		boolean receivedGray = false;
		System.out.println("Reducer start");
		StringBuffer sb = new StringBuffer();
		for (Text val : values) {
			// Create a node object to store the node details
			Node node = new Node(key.toString(), val.toString());
			sb.delete(0, sb.length());
			// If we get a black node for the first time, we will set the color
			// to black and fetch other details from the node
			if (node.getNodeColor().equals("BLACK") && receivedBlack == false) {
				// System.out.println("In first if");
				receivedBlack = true;
				thisNodeColor = node.getNodeColor();
				thisDistance = node.getDistanceFromSource();
				try {
					if (!node.getAdjListString().equals("null")) {
						thisAdjList = node.getAdjListString();
					}
				} catch (NullPointerException e) {
				}
				try {
					if (!node.getSourceNode().equals("null")) {

						if (!thisSourceNode.equalsIgnoreCase("source")) {
							thisSourceNode = node.getSourceNode();
						}
					}
				} catch (NullPointerException e) {
				}
				continue;
			}
			// If we already got a black node and got another black node, we
			// will compare both of them and select the minimum distance.
			if (node.getNodeColor().equals("BLACK") && receivedBlack == true) {
				thisDistance = Math.min(thisDistance,
						node.getDistanceFromSource());
				try {
					if (!node.getAdjListString().equals("null")) {
						thisAdjList = node.getAdjListString();
					}
				} catch (NullPointerException e) {
				}
				try {
					if (!node.getSourceNode().equals("null")) {

						if (!thisSourceNode.equalsIgnoreCase("source")) {
							thisSourceNode = node.getSourceNode();
						}
					}
				} catch (NullPointerException e) {
				}
				continue;

			}
			// If we get a gray node for the first time and didn't receive any
			// black nodes, then we will apply gray color and fetch other
			// details from node
			if (node.getNodeColor().equals("GRAY") && receivedBlack == false
					&& receivedGray == false) {
				receivedGray = true;
				thisNodeColor = node.getNodeColor();
				thisDistance = node.getDistanceFromSource();
				try {
					if (!node.getAdjListString().equals("null")) {
						thisAdjList = node.getAdjListString();
					}
				} catch (NullPointerException e) {
				}
				try {
					if (!node.getSourceNode().equals("null")) {

						if (!thisSourceNode.equalsIgnoreCase("source")) {
							thisSourceNode = node.getSourceNode();
						}
					}
				} catch (NullPointerException e) {
				}
				continue;
			}
			// If we receive gray node for the second time and no black node,
			// then we will compare the distance and consider the minimum
			// distance amongst all the gray nodes
			if (node.getNodeColor().equals("GRAY") && receivedBlack == false
					&& receivedGray == true) {
				thisDistance = Math.min(thisDistance,
						node.getDistanceFromSource());
				try {
					if (!node.getAdjListString().equals("null")) {
						thisAdjList = node.getAdjListString();
					}
				} catch (NullPointerException e) {
				}
				try {
					if (!node.getSourceNode().equals("null")) {

						if (!thisSourceNode.equalsIgnoreCase("source")) {
							thisSourceNode = node.getSourceNode();
						}
					}
				} catch (NullPointerException e) {
				}
				continue;
			}
			// If we get a gray node after getting a black node, we will not
			// update the color, but only try to fetch other information such as
			// adjacency list, source, etc
			if (node.getNodeColor().equals("GRAY") && receivedBlack == true) {
				try {
					if (!node.getAdjListString().equals("null")) {
						thisAdjList = node.getAdjListString();
					}
				} catch (NullPointerException e) {
				}
				try {
					if (!node.getSourceNode().equals("null")) {

						if (!thisSourceNode.equalsIgnoreCase("source")) {
							thisSourceNode = node.getSourceNode();
						}
					}
				} catch (NullPointerException e) {
				}
				continue;
			}
			// If we get a white node after getting black or gray node, fetch
			// adjacency list and source information
			if (node.getNodeColor().equals("WHITE")
					&& (receivedGray == true || receivedBlack == true)) {
				try {
					if (!node.getAdjListString().equals("null")) {
						thisAdjList = node.getAdjListString();
					}
				} catch (NullPointerException e) {
				}
				continue;
			}
			// If we get only a white node without any gray or black node, send
			// it through
			if (node.getNodeColor().equals("WHITE")
					&& (receivedGray == false || receivedBlack == false)) {
				thisAdjList = node.getAdjListString();
				thisDistance = node.getDistanceFromSource();
				thisNodeColor = node.getNodeColor();
				thisSourceNode = node.getSourceNode();
				continue;
			}
		}
		// Append all the node details
		sb.append(thisAdjList + "|" + thisDistance + "|" + thisNodeColor + "|"
				+ thisSourceNode);
		// If there is at least one gray node in the output list, set the
		// MORE_ITERATIONS value to 1, which means more iterations are required
		if (thisNodeColor.equalsIgnoreCase("GRAY")) {
			context.getCounter(PROGRAM_COUNTERS.MORE_ITERATIONS).setValue(1);
			System.out.println("More Iterations: "
					+ context.getCounter(PROGRAM_COUNTERS.MORE_ITERATIONS)
							.getValue());
			System.out.println("Continue the loop");
		}
		context.write(key, new Text(sb.toString()));
	}
}