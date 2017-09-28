package org.htmedia.shine.simprofileknn.archieve;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import org.apache.hadoop.io.IntWritable;

public class NPhase2Partitioner_v02 extends Configured implements Tool {

	public static final Log LOG = LogFactory.getLog(NPhase2Partitioner_v02.class);

	public static class PartitionerMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		int dimension;

		public void configure(JobConf job) {
			System.out.println("Map Task configuration started ....");
			LOG.info("Map Task configuration started ....");
			dimension = job.getInt("dimension", 7);
			System.out.println("Map Task configured ...");
			LOG.info("Map Task configured ...");
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] simCandSims = value.toString().split(",");

			try {
				if (!simCandSims[0].equals("null")
						&& !simCandSims[1].equals("null")) {

					String simCandKey = simCandSims[0] + "," + simCandSims[1]
							+ "," + simCandSims[3];
					String simCandSimsValues = simCandSims[2];
					simCandSimsValues += "," + simCandSims[3];
					int flag = 4;
					for (int i = 0; i < dimension; i++) {
						simCandSimsValues += "," + simCandSims[i + flag];
					}
					output.collect(new Text(simCandKey), new Text(
							simCandSimsValues));
				}
			} catch (Exception e) {
				System.out.println("Error with key (u1,u2,fa): "
						+ simCandSims[0] + "," + simCandSims[1] + ","
						+ simCandSims[3]);
				LOG.info("Error with key (u1,u2,fa): " + simCandSims[0] + ","
						+ simCandSims[1] + "," + simCandSims[3]);
			}
		}
	}

	public static class FAPartitioner implements Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {

			String[] simDetails = key.toString().split(",");
			int fa = Integer.parseInt(simDetails[2]);
			if (fa == -1) {
				return numReduceTasks;
			} else
				return fa;
		}

		public void configure(JobConf job) {
			System.out.println("Partition Task configuration started ....");
			LOG.info("Partition Task configuration started ....");
			System.out.println("Partition Task configured ...");
			LOG.info("Partition Task configured ...");
		}
	}

	public static class PartitionerReducer extends MapReduceBase implements
			Reducer<Text, Text, NullWritable, Text> {

		public void configure(JobConf job) {
			System.out.println("Reduce Task configuration started ....");
			LOG.info("Reduce Task configuration started ....");
			System.out.println("Reduce Task configured ...");
			LOG.info("Reduce Task configured ...");
		}

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {

				String[] simCand = key.toString().split(",");

				try {
					String finalkey = simCand[0] + "," + simCand[1];
					Text val = values.next();
					output.collect(NullWritable.get(), new Text(finalkey + ","
							+ val.toString()));
				} catch (Exception e) {
					System.out.println("Error with key (u1,u2,fa): "
							+ simCand[0] + "," + simCand[1] + "," + simCand[2]);
					LOG.info("Error with key (u1,u2,fa): " + simCand[0] + ","
							+ simCand[1] + "," + simCand[2]);
				}

			}
		}
	}

	static int printUsage() {
		System.out
				.println("NPhase2Partitioner [-map <maps>] [-red <reduces>] [-dim <dimension>]"
						+ "<input> <output>");
		LOG.info("NPhase2Partitioner [-map <maps>] [-red <reduces>] [-dim <dimension>]"
				+ "<input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), NPhase2Partitioner_v02.class);
		conf.setJobName("NPhase2Partitioner");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PartitionerMapper.class);
		conf.setPartitionerClass(FAPartitioner.class);
		conf.setReducerClass(PartitionerReducer.class);

		List<String> other_args = new ArrayList<String>();

		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-map".equals(args[i])) {
					++i;
				} else if ("-red".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else if ("-dim".equals(args[i])) {
					conf.setInt("dimension", Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "
						+ args[i]);
				LOG.info("NPhase2Partitioner [-map <maps>] [-red <reduces>] [-dim <dimension>]"
						+ "<input> <output>");
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "
						+ args[i - 1]);
				LOG.info("ERROR: Required parameter missing from "
						+ args[i - 1]);
				return printUsage();
			}
		}

		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 2.");
			LOG.info("ERROR: Wrong number of parameters: " + other_args.size()
					+ " instead of 2.");
			return printUsage();
		}

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("NPhase2Partition Run Started ...");
		LOG.info("NPhase2Partition Run Started ...");
		int res = ToolRunner.run(new Configuration(), new NPhase2Partitioner_v02(),
				args);
	}
}