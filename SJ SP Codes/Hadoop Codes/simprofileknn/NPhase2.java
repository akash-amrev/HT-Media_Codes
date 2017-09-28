/* Previous Version - v05
 * 
 * This Version - Logging
 * 				-  
 */

package org.htmedia.shine.simprofileknn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Phase2 of Similar Candidate Block Nested Loop KNN Method.
 */
public class NPhase2 extends Configured implements Tool {
	
	public static final Log LOG = LogFactory.getLog(NPhase2.class);
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, NPhase2Value> {

		int dimension;

		public void configure(JobConf job) {
			System.out.println("Map Task configuration started ....");
			LOG.info("Map Task configuration started ....");
			dimension = job.getInt("dimension", 7);
			System.out.println("Map Task configured ...");
			LOG.info("Map Task configured ...");
		}

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, NPhase2Value> output,
				Reporter reporter) throws IOException {
			String line = value.toString();
			String[] parts = line.split(",");
			// key format <rid1>
			IntWritable mapKey = new IntWritable(Integer.valueOf(parts[0]));
			try {
				// value format <rid2, dist>
				String simVals = parts[3] + ",";
				for (int i = 0; i < (dimension - 1); i++) {
					simVals += Integer.toString((int) (1000 * Float
							.valueOf(parts[4 + i]))) + ",";
				}
				simVals += Integer.toString((int) (1000 * Float
						.valueOf(parts[4 + (dimension - 1)])));

				NPhase2Value np2v = new NPhase2Value(Integer.valueOf(parts[1]),
						(int) (1000 * Float.valueOf(parts[2])), new Text(
								simVals));
				output.collect(mapKey, np2v);
			} catch (Exception e) {
				System.out.println("Error for id : " + parts[0]);
				LOG.info("Error for id : " + parts[0]);
				e.printStackTrace();
			}

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, NPhase2Value, NullWritable, Text> {
		int numberOfPartition;
		int knn;

		class Record {
			public int id2;
			public int dist;
			public String simVals;

			Record(int id2, int dist, String simVals) {
				this.id2 = id2;
				this.dist = dist;
				this.simVals = simVals;
			}

			public String toString() {
				return Integer.toString(id2) + "," + Integer.toString(dist)
						+ "," + simVals;
			}
		}

		class RecordComparator implements Comparator<Record> {
			public int compare(Record o1, Record o2) {
				int ret = 0;
				int dist = o2.dist - o1.dist;

				if (Math.abs(dist) < 1E-6)
					ret = o1.id2 - o2.id2;
				else if (dist > 0)
					ret = 1;
				else
					ret = -1;
				return -ret;
			}
		}

		public void configure(JobConf job) {
			System.out.println("Reduce Task configuration started ...");
			LOG.info("Reduce Task configuration started ...");
			numberOfPartition = job.getInt("numberOfPartition", 2);
			knn = job.getInt("knn", 3);
			System.out.println("Reduce Task Configured ...");
			LOG.info("Reduce Task Configured ...");
		}

		public void reduce(IntWritable key, Iterator<NPhase2Value> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			RecordComparator rc = new RecordComparator();
			PriorityQueue<Record> pq = new PriorityQueue<Record>(knn + 1, rc);

			// For each record we have a reduce task
			// value format <rid1, rid2, dist>
			while (values.hasNext()) {
				NPhase2Value np2v = values.next();
				int id2 = np2v.getFirst().get();
				try{
				int dist = np2v.getSecond().get();
				String simVals = np2v.getThird().toString();
				Record record = new Record(id2, dist, simVals);
				pq.add(record);
				if (pq.size() > knn)
					pq.poll();
				} catch (Exception e) {
					System.out.println("Error for id : " + Integer.toString(id2));
					LOG.info("Error for id : " + Integer.toString(id2));
					e.printStackTrace();
				}

			}

			while (pq.size() > 0) {
				output.collect(NullWritable.get(), new Text(key.toString()
						+ "," + pq.poll().toString()));
				// break; // only ouput the first record
			}
		} // reduce
	} // Reducer

	static int printUsage() {
		System.out
				.println("NPhase1 [-map <maps>] [-red <reduces>] [-part <numberOfPartitions>] [-dim <dimension>]"
						+ "[-knn <knn>] " + "<input> <output>");
		LOG.info("NPhase1 [-map <maps>] [-red <reduces>] [-part <numberOfPartitions>] [-dim <dimension>]"
				+ "[-knn <knn>] " + "<input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * The main driver for Similar Candidate program. Invoke this method to
	 * submit the map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), NPhase2.class);
		conf.setJobName("NPhase2");

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(NPhase2Value.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);

		int numberOfPartition = 0;
		List<String> other_args = new ArrayList<String>();

		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-map".equals(args[i])) {
					++i;
				} else if ("-red".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else if ("-part".equals(args[i])) {
					numberOfPartition = Integer.parseInt(args[++i]);
					conf.setInt("numberOfPartition", numberOfPartition);
				} else if ("-dim".equals(args[i])) {
					conf.setInt("dimension", Integer.parseInt(args[++i]));
				} else if ("-knn".equals(args[i])) {
					int knn = Integer.parseInt(args[++i]);
					conf.setInt("knn", knn);
					System.out.println("K Nearest : " + knn);
					LOG.info("K Nearest : " + knn);
				} else {
					other_args.add(args[i]);
				}
				conf.setNumReduceTasks(numberOfPartition * numberOfPartition);
				// conf.setNumReduceTasks(1);
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "
						+ args[i]);
				LOG.info("ERROR: Integer expected instead of "
						+ args[i]);
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
			LOG.info("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 2.");
			return printUsage();
		}

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("NPhase2 Run Started ...");
		LOG.info("NPhase2 Run Started ...");
		int res = ToolRunner.run(new Configuration(), new NPhase2(), args);
	}
} // NPhase2