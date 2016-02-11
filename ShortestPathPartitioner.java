package pkg;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ShortestPathPartitioner extends Partitioner<Text, Text> {
	/*
	 * Partitioner will automatically take the number of Reduce tasks and routes
	 * the mapper output equally among all the reducers.
	 */

	public int getPartition(Text key, Text values, int numReduceTasks) {
		System.out.println("Partitioner start");
		int node = Integer.parseInt(key.toString());
		if (Integer.parseInt(key.toString()) <= (numReduceTasks - 1)) {
			System.out.println("Partitioner : " + node);
			return node;
		}

		else {
			for (int i = 0; i < numReduceTasks; i++) {
				if (node % numReduceTasks == i) {
					if (i == 0) {
						System.out.println("Partitioner : "
								+ (numReduceTasks - 1));
						return numReduceTasks - 1;
					} else {
						System.out.println("Partitioner : " + (i - 1));
						return i - 1;
					}
				}
			}
			// Return first partitioner if no condition is satisfied
			return 0;
		}
	}

}
