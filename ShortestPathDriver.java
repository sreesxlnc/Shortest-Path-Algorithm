package pkg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pkg.ShortestPathMapper.PROGRAM_COUNTERS;

public class ShortestPathDriver extends Configured implements Tool {
	public int run(String args[]) throws Exception {
		// Setting up summary job to print the node summary in Node_Summary
		// folder
		Configuration confSummary = this.getConf();
		Job summaryJob = new Job(confSummary, "Node_Summary_Job");
		summaryJob.setJarByClass(ShortestPathDriver.class);
		summaryJob.setMapperClass(NodeSummaryMapper.class);
		summaryJob.setReducerClass(NodeSummaryReducer.class);
		FileInputFormat.addInputPath(summaryJob, new Path(args[0]));
		Path nodeSummaryPath = new Path("Node_Summary");
		FileOutputFormat.setOutputPath(summaryJob, nodeSummaryPath);
		summaryJob.setOutputKeyClass(Text.class);
		summaryJob.setOutputValueClass(Text.class);
		summaryJob.waitForCompletion(true);

		/*
		 * Setting up first job to calculate shortest path. The input is taken
		 * from the first argument. Output folders will be named as argument 1
		 * appended with Iteration number
		 */
		Configuration conf = this.getConf();
		Job job = new Job(conf, "ShortestPath_Iteration1");

		job.setJarByClass(ShortestPathDriver.class);
		job.setMapperClass(ShortestPathMapper.class);
		job.setReducerClass(ShortestPathReducer.class);
		job.setPartitionerClass(ShortestPathPartitioner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		// Appending "_Iteration_1" to argument 1
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_Iteration_"
				+ 1));
		job.waitForCompletion(true);
		// Variable to count the number of iteration
		long iterationNumber = 1;
		// Looping and running the jobs in chain until the counter
		// MORE_ITERATIONS becomes zero
		while (job.getCounters().findCounter(PROGRAM_COUNTERS.MORE_ITERATIONS)
				.getValue() > 0) {
			iterationNumber++;
			System.out.println("Iteration: " + iterationNumber);
			conf = new Configuration();
			conf.set("iteration_depth", " " + iterationNumber);
			job = new Job(conf, "ShortestPath_Iteration" + iterationNumber);
			job.setJarByClass(ShortestPathDriver.class);
			job.setMapperClass(ShortestPathMapper.class);
			job.setReducerClass(ShortestPathReducer.class);
			job.setPartitionerClass(ShortestPathPartitioner.class);
			job.setNumReduceTasks(3);
			// Input folder is the output folder of previous job
			Path inputPath = new Path(args[1] + "_Iteration_"
					+ (iterationNumber - 1));
			Path outputPath = new Path(args[1] + "_Iteration_"
					+ (iterationNumber));
			FileInputFormat.addInputPath(job, inputPath);
			FileSystem fs = FileSystem.get(conf);
			// Delete the output path if it already exists
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(job, outputPath);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.waitForCompletion(true);

		}

		return 1;
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ShortestPathDriver(),
				args);
		System.exit(res);
	}
}
