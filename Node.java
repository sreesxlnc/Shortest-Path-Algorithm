package pkg;

import java.util.ArrayList;

public class Node {
	private String nodeName;
	private ArrayList<String> adjacencyList = new ArrayList<String>();
	private String adjListString;
	private int distanceFromSource;
	private String nodeColor;
	private String sourceNode;

	public Node(String node, String details) {
		nodeName = node;
		String[] nodeDetails = details.split("\\|");

		for (int i = 0; i < nodeDetails.length; i++) {
			System.out.println("Node details : " + i + "  " + nodeDetails[i]);
		}
		if (nodeDetails.length == 0) {
			System.exit(-1); // -1 implies exit after an error
		}
		adjListString = nodeDetails[0];
		System.out.println("Length of adjListString : "
				+ adjListString.length());
		System.out.println("adjListString : " + adjListString);
		System.out.println(" String.equals output : "
				+ adjListString.equals("null"));
		String[] adjListTemp = nodeDetails[0].split("\\,");
		for (int i = 0; i < adjListTemp.length; i++) {
			adjacencyList.add(adjListTemp[i]);
		}
		System.out.println("Adjacency List : " + adjacencyList);
		System.out.println("Length of Adj List: " + adjacencyList.size());

		// Assigning distance from source
		try {
			distanceFromSource = Integer.parseInt(nodeDetails[1]);
		} catch (NumberFormatException e) {
			distanceFromSource = Integer.MAX_VALUE;
		}
		System.out.println("Distance from source : " + distanceFromSource);
		System.out.println("Test statement1");
		// Assigning node color
		if (nodeDetails[2].equals("BLACK") || nodeDetails[2].equals("WHITE")
				|| nodeDetails[2].equals("GRAY")) {
			System.out.println("Test statement2");
			nodeColor = nodeDetails[2];

		} else {
			System.out.println("Test statement3");
			System.err.print("Unknown color");
			System.exit(-1);
		}

		System.out.println("Node color : " + nodeColor);
		// Assigning source node
		sourceNode = nodeDetails[3];
		System.out.println("Source node: " + sourceNode);
	}

	// Getters and setters for private variables which store node details

	public String getNodeColor() {
		return nodeColor;
	}

	public void setNodeColor(String nodeColor) {
		this.nodeColor = nodeColor;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public ArrayList<String> getAdjacencyList() {
		return adjacencyList;
	}

	public void setAdjacencyList(ArrayList<String> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}

	public int getDistanceFromSource() {
		return distanceFromSource;
	}

	public void setDistanceFromSource(int distanceFromSource) {
		this.distanceFromSource = distanceFromSource;
	}

	public String getSourceNode() {
		return sourceNode;
	}

	public void setSourceNode(String sourceNode) {
		this.sourceNode = sourceNode;
	}

	public String getAdjListString() {
		return adjListString;
	}

	public void setAdjListString(String adjListString) {
		this.adjListString = adjListString;
	}
	/*
	 * 
	 * public static void main(String args[]) throws Exception { Node node = new
	 * Node("1", "null|2|GRAY|4"); }
	 */
}
