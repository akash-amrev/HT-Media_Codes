package org.htmedia.shine.simprofileknn;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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

public class NPhase2PartitionerLookUp extends Configured implements Tool {

	public static final Log LOG = LogFactory
			.getLog(NPhase2PartitionerLookUp.class);

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
						+ value.toString());
				LOG.info("Error with key (u1,u2,fa): " + value.toString());
			}
		}
	}

	public static class FAPartitioner implements Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {

			String[] simDetails = key.toString().split(",");
			int fa = Integer.parseInt(simDetails[2]);
			if (fa == -1) {
				return numReduceTasks - 1;
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

		private HashMap<Integer, HashMap<String, Integer>> userLookup;
		private Path[] localFiles;

		public void configure(JobConf job) {
			System.out.println("Reduce Task configuration started ....");
			LOG.info("Reduce Task configuration started ....");
			// Distributed Cache File
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			userLookup = itemDeSerializationFAWise(localFiles[0].toString());
			// userLookup =
			// itemDeSerializationFAWise("/home/cloudera/Documents/similarprofiles/Input/UserID_Lookup_FAwise_sample.ser");
			System.out.println("Reduce Task configured ...");
			LOG.info("Reduce Task configured ...");
		}

		@SuppressWarnings("unchecked")
		public static HashMap<Integer, HashMap<String, Integer>> itemDeSerializationFAWise(
				String filePath) {
			long t1 = System.currentTimeMillis();
			FileInputStream fis;
			ObjectInputStream ois;
			HashMap<Integer, HashMap<String, Integer>> map = null;
			try {
				fis = new FileInputStream(filePath);
				ois = new ObjectInputStream(fis);
				map = (HashMap<Integer, HashMap<String, Integer>>) ois
						.readObject();
				System.out
						.println("Time Taken to DeSerialize the Object : "
								+ Long.toString((System.currentTimeMillis() - t1) / 1000));
				LOG.info("Time Taken to DeSerialize the Object : "
						+ Long.toString((System.currentTimeMillis() - t1) / 1000));
				ois.close();
				fis.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return map;
		}

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {

				String[] simCand = key.toString().split(",");

				try {
					String user1 = simCand[0];
					String user2 = simCand[1];
					int fa = Integer.parseInt(simCand[2]);

					String finalkey = Integer.toString(userLookup.get(fa).get(
							user1))
							+ ","
							+ Integer.toString(userLookup.get(fa).get(user2));
					Text val = values.next();
					output.collect(NullWritable.get(), new Text(finalkey + ","
							+ val.toString()));
				} catch (Exception e) {
					System.out.println("Error with key (u1,u2,fa): "
							+ key.toString());
					LOG.info("Error with key (u1,u2,fa): " + key.toString());
				}

			}
		}
	}

	static int printUsage() {
		System.out
				.println("NPhase2PartitionerLookUp [-map <maps>] [-red <reduces>] [-dim <dimension>]"
						+ "<path> <input> <output>");
		LOG.info("NPhase2PartitionerLookUp [-map <maps>] [-red <reduces>] [-dim <dimension>]"
				+ "<path> <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), NPhase2PartitionerLookUp.class);
		conf.setJobName("NPhase2PartitionerLookUp");

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

		// Make sure there are exactly 3 parameters left.
		if (other_args.size() != 3) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 3.");
			LOG.info("ERROR: Wrong number of parameters: " + other_args.size()
					+ " instead of 3.");
			return printUsage();
		}

		DistributedCache.addCacheFile(new URI(other_args.get(0)
				+ "UserID_Lookup_FAwise.ser"), conf);

		FileInputFormat.setInputPaths(conf, other_args.get(1));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(2)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("NPhase2PartitionerLookUp Run Started ...");
		LOG.info("NPhase2PartitionerLookUp Run Started ...");
		int res = ToolRunner.run(new Configuration(),
				new NPhase2PartitionerLookUp(), args);
	}
}