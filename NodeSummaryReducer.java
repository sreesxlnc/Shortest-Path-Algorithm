package pkg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NodeSummaryReducer extends Reducer<Text, Text, Text, Text> {
	/*
	 * Reducer class for the first job in the sequence. This Reducer simply
	 * writes the information Mapper
	 */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Node Summary Reducer Start");
		for (Text value : values) {
			context.write(key, value);
		}
	}
}
