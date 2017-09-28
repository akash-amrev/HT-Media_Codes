/*
 * THERE IS NO WARRANTY FOR THE PROGRAM, TO THE EXTENT PERMITTED BY APPLICABLE LAW.
 * EXCEPT WHEN OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER
 * PARTIES PROVIDE THE PROGRAM "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
 * EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE ENTIRE RISK AS
 * TO THE QUALITY AND PERFORMANCE OF THE PROGRAM IS WITH YOU. SHOULD THE PROGRAM
 * PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL NECESSARY SERVICING, REPAIR
 * OR CORRECTION.
 */

package org.htmedia.shine.simprofileknn.archieve;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Phase1 of Hadoop Block Nested Loop KNN Join (H-BNLJ).
 */
public class NPhase1_v01 extends Configured implements Tool {
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, NPhase1Value> {
		private int numberOfPartition;
		private int dimension;
		private int scale = 1000;
		private int fileId = 0;
		private String inputFile;
		private String mapTaskId;
		private Random r;
		private int recIdOffset;
		private int binOffset;
		private int coordOffset;
		private float[] weightSim = new float[] { 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
				0.0f, 0.0f };

		public void configure(JobConf job) {
			inputFile = job.get("map.input.file");
			mapTaskId = job.get("mapred.task.id");
			numberOfPartition = job.getInt("numberOfPartition", 2);
			dimension = job.getInt("dimension", 2);

			recIdOffset = 0;
			binOffset = recIdOffset + 1;
			coordOffset = recIdOffset + 6;

			r = new Random();

			if (inputFile.indexOf("outer") != -1)
				fileId = 0;
			else if (inputFile.indexOf("inner") != -1)
				fileId = 1;
			else {
				System.out.println("Invalid input file source@NPhase1");
				System.exit(-1);
			}
		} // configure

		/**
		 * Partition the input data sets (R and S) into multiple buckets.
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, NPhase1Value> output,
				Reporter reporter) throws IOException {
			String[] parts = value.toString().split(" +");
			String recId = parts[recIdOffset];
			int recIdInt = Integer.parseInt(recId);
			String[] bin = new String[5];
			for (int i = 0; i < 5; i++)
				bin[i] = parts[binOffset + i];
			float[] coord = new float[dimension];
			for (int i = 0; i < dimension; i++)
				coord[i] = Float.parseFloat(parts[coordOffset + i]);

			NPhase1Value np1v = new NPhase1Value(recIdInt, bin, coord,
					(byte) fileId, dimension);

			// Random generate a partition ID for an input record
			int partID = r.nextInt(numberOfPartition);
			int groupID = 0;

			for (int i = 0; i < numberOfPartition; i++) {
				if (fileId == 0)
					groupID = partID * numberOfPartition + i;
				else if (fileId == 1)
					groupID = partID + i * numberOfPartition;
				else {
					System.out.println("The record comes from unknow file!!!");
					System.exit(-1);
				}

				IntWritable mapKey = new IntWritable(groupID);
				output.collect(mapKey, np1v);
			}
			// System.gc();
		} // map
	} // MapClass

	/**
	 * Perform Block Nested Loop join for records in the same partition/bucket.
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, NPhase1Value, NullWritable, Text> {
		private int bufferSize = 8 * 1024 * 1024;
		private MultipleOutputs mos;

		private LocalDirAllocator lDirAlloc = new LocalDirAllocator(
				"mapred.local.dir");
		private FSDataOutputStream out;
		private FileSystem localFs;
		private FileSystem lfs;
		private Path file1;
		private Path file2;

		private int numberOfPartition;
		private int dimension;
		private int blockSize;
		private int knn;
		private String weightSimString;
		private float[] weightSim = new float[] { 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
				0.0f, 0.0f };
		private Path[] localFiles;
		private HashMap<Integer, HashMap<Integer, Float>> indMap;
		private HashMap<Integer, HashMap<Integer, Float>> subFAMap;
		private HashMap<Integer, HashMap<Integer, Float>> instiMap;
		private HashMap<Integer, HashMap<Integer, Float>> jtMap;

		private int binSize = 5;

		// private boolean self_join;

		private Configuration jobinfo;

		public void configure(JobConf job) {
			numberOfPartition = job.getInt("numberOfPatition", 2);
			dimension = job.getInt("dimension", 1);
			blockSize = job.getInt("blockSize", 1024);
			knn = job.getInt("knn", 1024);
			weightSimString = job.get("weightSim");
			String[] weightDummy = weightSimString.toString().split(" +");
			for (int i = 0; i < 7; i++) {
				weightSim[i] = Float.parseFloat(weightDummy[i]);
			}

			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			// Get the local file system
			try {
				localFs = FileSystem.getLocal(job);
			} catch (IOException e) {
				e.printStackTrace();
			}

			lfs = ((LocalFileSystem) localFs).getRaw();
			jobinfo = job;
			// mos = new MultipleOutputs(job);
		}

		public static HashMap<Integer, HashMap<Integer, Float>> itemDeSerialization(
				String filePath) {
			long t1 = System.currentTimeMillis();
			HashMap<Integer, HashMap<Integer, Float>> map = null;
			try {
				FileInputStream fin = new FileInputStream(filePath);
				ObjectInputStream ois = new ObjectInputStream(fin);
				map = (HashMap<Integer, HashMap<Integer, Float>>) ois
						.readObject();
				// System.out.println("Time Taken to DeSerialize the Object : "
				// + Long.toString(System.currentTimeMillis() - t1));
				fin.close();
				ois.close();
			} catch (FileNotFoundException e1) {
				System.err.println("Serialization File Error");
				System.exit(0);
				e1.printStackTrace();
			} catch (IOException e) {
				System.err.println("Serialization Output Stream Error");
				System.exit(0);
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return map;
		}

		public static float distance(float[] p1, float[] p2) {
			float dotProduct = 0.0f;
			float lengthSquaredp1 = 0.0f;
			float lengthSquaredp2 = 0.0f;
			for (int i = 0; i < p1.length; i++) {
				lengthSquaredp1 += p1[i] * p1[i];
				lengthSquaredp2 += p2[i] * p2[i];
				dotProduct += p1[i] * p2[i];
			}
			float denominator = (float) (Math.sqrt(lengthSquaredp1
					* lengthSquaredp2));

			// correct for floating-point rounding errors
			if (denominator < dotProduct) {
				denominator = dotProduct;
			}

			// correct for zero-vector corner case
			if (denominator == 0 && dotProduct == 0) {
				return 0;
			}
			p1 = null;
			p2 = null;
			return dotProduct / denominator;
		}

		public void reduce(IntWritable key, Iterator<NPhase1Value> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			String algorithm = "nested_loop";
			String prefix_dir = algorithm + "-"
					+ Integer.toString(numberOfPartition) + "-"
					+ key.toString();
			long t1 = System.currentTimeMillis();

			// System.out.println(localFiles[0].getName());
			// System.out.println(localFiles[0].getParent());

			indMap = itemDeSerialization(localFiles[0].toString());
			subFAMap = itemDeSerialization(localFiles[1].toString());
			instiMap = itemDeSerialization(localFiles[2].toString());
			jtMap = itemDeSerialization(localFiles[3].toString());

			// indMap =
			// itemDeSerialization("/home/cloudera/Documents/similarprofiles/Input/ind_ind_sim.ser");
			// subFAMap =
			// itemDeSerialization("/home/cloudera/Documents/similarprofiles/Input/subfa_subfa_sim.ser");
			// instiMap =
			// itemDeSerialization("/home/cloudera/Documents/similarprofiles/Input/insti_insti_sim.ser");
			// jtMap =
			// itemDeSerialization("/home/cloudera/Documents/similarprofiles/Input/jt_jt_sim.ser");

			// System.out.println(indMap.size());
			// System.out.println(subFAMap.size());
			// System.out.println(instiMap.size());
			// System.out.println(jtMap.size());

			try {
				file1 = lDirAlloc.getLocalPathForWrite(prefix_dir + "/"
						+ "outer", jobinfo);
				file2 = lDirAlloc.getLocalPathForWrite(prefix_dir + "/"
						+ "inner", jobinfo);
				lfs.create(file1);
				lfs.create(file2);
			} catch (IOException e) {
				e.printStackTrace();
			}

			String outerTable = file1.toString();
			String innerTable = file2.toString();
			FileWriter fwForR = new FileWriter(outerTable);
			FileWriter fwForS = new FileWriter(innerTable);
			BufferedWriter bwForR = new BufferedWriter(fwForR, bufferSize);
			BufferedWriter bwForS = new BufferedWriter(fwForS, bufferSize);

			long t2 = System.currentTimeMillis();
			while (values.hasNext()) {
				// Value format <rid, coord, src>

				NPhase1Value np1v = values.next();
				String record = np1v.getFirst().toString();
				StringBuilder recordbuilder = new StringBuilder(5 * dimension);
				recordbuilder.append(record);
				String[] partsSecond = np1v.getSecond().toStrings();
				String[] partsThird = np1v.getThird().toStrings();
				int srcId = (int) np1v.getFourth().get();

				for (int i = 0; i < binSize; i++)
					recordbuilder.append(" " + partsSecond[i]);
				for (int i = 0; i < dimension; i++)
					recordbuilder.append(" " + partsThird[i]);
				if (srcId == 0) {
					bwForR.write(recordbuilder.toString() + "\n");
				} else if (srcId == 1) {
					bwForS.write(recordbuilder.toString() + "\n");
				} else {
					System.out.println("unknown file number");
					System.exit(-1);
				}
				partsSecond = null;
				partsThird = null;
				recordbuilder = null;
			}

			System.out.println("Time Taken By Reducer to Spill Records: "
					+ Long.toString((System.currentTimeMillis() - t2) / 1000));

			reporter.progress();
			bwForR.close();
			bwForS.close();
			fwForR.close();
			fwForS.close();

			FileReader frForR = new FileReader(outerTable);
			BufferedReader brForR = new BufferedReader(frForR, bufferSize);

			// initialize for R
			int number = blockSize;
			int[] idR = new int[number];
			String[][] binR = new String[number][binSize];
			float[][] coordR = new float[number][dimension];
			ArrayList<PriorityQueue> knnQueueR = new ArrayList<PriorityQueue>(
					number);

			// Create priority queue with specified comparator
			Comparator<ListElem> rc = new RecordComparator();
			for (int j = 0; j < number; j++) {
				PriorityQueue<ListElem> knnQueue = new PriorityQueue<ListElem>(
						knn + 1, rc);
				knnQueueR.add(knnQueue);
			}

			boolean flag = true;
			while (flag) {
				// Read a block of R
				long t3 = System.currentTimeMillis();
				for (int ii = 0; ii < number; ii++) {
					String line = brForR.readLine();
					if (line == null) {
						flag = false; // Going to end
						number = ii;
						break;
					}

					String parts[] = line.split(" +");
					int id1 = Integer.parseInt(parts[0]);
					idR[ii] = id1;

					int st = 1;
					String[] x_bin = binR[ii];
					for (int i = 0; i < binSize; i++)
						x_bin[i] = parts[st + i];
					st = binSize + 1;
					float[] x = coordR[ii];
					for (int i = 0; i < dimension; i++)
						x[i] = Float.parseFloat(parts[st + i]);
				}

				// if (self_join) innerTable = outerTable;
				// For all records in a block of R, the following carries out
				// knn-join with S
				FileReader frForS = new FileReader(innerTable);
				BufferedReader brForS = new BufferedReader(frForS, bufferSize);

				while (true) {
					String line = brForS.readLine();
					if (line == null)
						break;
					String parts[] = line.split(" +");
					int id2 = Integer.parseInt(parts[0]);

					int st = 1;
					String[] y_bin = new String[binSize];
					for (int i = 0; i < binSize; i++)
						y_bin[i] = parts[st + i];
					st = binSize + 1;

					float[] y = new float[dimension];
					for (int i = 0; i < dimension; i++)
						y[i] = Float.parseFloat(parts[st + i]);

					parts = null;

					float distArray = 0.0f;
					for (int i = 0; i < number; i++) {
						distArray = 0;
						float factor;
						float simval;
						float dummy = 0.0f;
						String[] x_bin = binR[i];
						float[] x = coordR[i];

						if (Integer.parseInt(x_bin[0]) != Integer
								.parseInt(y_bin[0]))
							continue;

						if (y_bin[2].indexOf(x_bin[1]) == -1)
							continue;

						if (y_bin[4].indexOf(x_bin[3]) == -1)
							continue;

						StringBuilder simVals = new StringBuilder(250);
						// if (x[0] != y[0]) // To handle bin similarity
						// continue;
						for (int k = 0; k < 6; k++) {
							if (x[k] == -1 || y[k] == -1) {
								distArray += 0.0;
								simVals.append(Float.toString(0.0f) + " ");
							} else {
								float xdummy = x[k];
								float ydummy = y[k];
								switch (k) {
								case 0:
									if (xdummy == 0.0f || ydummy == 0.0f) {
										if (ydummy == 0.0f) {
											dummy = xdummy;
										} else {
											dummy = ydummy;
										}
										simval = (float) Math.exp(-2 * dummy);
										distArray += weightSim[k] * simval;
										simVals.append(Float.toString(simval)
												+ " ");
									} else {
										if (xdummy > ydummy) {
											dummy = ydummy;
											ydummy = xdummy;
											xdummy = dummy;
										}
										factor = (ydummy - xdummy) / (xdummy);
										simval = (float) Math.exp(-1 * 0.4
												* (Math.pow(factor, 2))
												* xdummy);
										distArray += weightSim[k] * simval;
										simVals.append(Float.toString(simval)
												+ " ");
									}
									break;
								case 1:
									if (xdummy == 0 || ydummy == 0) {
										if (ydummy == 0) {
											dummy = xdummy;
										} else {
											dummy = ydummy;
										}
										simval = (float) Math.exp(-dummy);
										distArray += weightSim[k] * simval;
										simVals.append(Float.toString(simval)
												+ " ");
									} else {
										if (xdummy > ydummy) {
											dummy = ydummy;
											ydummy = xdummy;
											xdummy = dummy;
										}
										factor = (ydummy - xdummy) / (xdummy);
										simval = (float) Math
												.exp(-1
														* 1.3
														* (0.2 * Math.pow(
																factor, 4) + 0.2 * Math
																.pow(factor, 2))
														* xdummy);
										distArray += weightSim[k] * simval;
										simVals.append(Float.toString(simval)
												+ " ");
									}
									break;
								case 2:
									if (xdummy > ydummy) {
										dummy = ydummy;
										ydummy = xdummy;
										xdummy = dummy;
									}

									if (indMap.get((int) xdummy).containsKey(
											(int) ydummy)) {
										simval = indMap.get((int) xdummy).get(
												(int) ydummy);
										distArray += weightSim[k] * simval;
										simVals.append(Float.toString(simval)
												+ " ");
									} else {
										distArray += 0.0;
										simVals.append(Float.toString(0.0f)
												+ " ");
									}
									break;
								case 3:
									if (xdummy > ydummy) {
										dummy = ydummy;
										ydummy = xdummy;
										xdummy = dummy;
									}
									if (subFAMap.get((int) xdummy).containsKey(
											(int) ydummy)) {
										simval = subFAMap.get((int) xdummy)
												.get((int) ydummy);
										distArray += weightSim[k] * simval;
										simVals.append(Float.toString(simval)
												+ " ");
									} else {
										distArray += 0.0;
										simVals.append(Float.toString(0.0f)
												+ " ");
									}
									break;
								case 4:
									if (xdummy > ydummy) {
										dummy = ydummy;
										ydummy = xdummy;
										xdummy = dummy;
									}
									if (instiMap.get((int) xdummy).containsKey(
											(int) ydummy)) {
										simval = instiMap.get((int) xdummy)
												.get((int) ydummy);
										distArray += weightSim[k] * simval;
										simVals.append(Float.toString(simval)
												+ " ");
									} else {
										distArray += 0.0;
										simVals.append(Float.toString(0.0f)
												+ " ");
									}
									break;
								case 5:
									if (xdummy > ydummy) {
										dummy = ydummy;
										ydummy = xdummy;
										xdummy = dummy;
									}
									if (jtMap.get((int) xdummy).containsKey(
											(int) ydummy)) {
										simval = jtMap.get((int) xdummy).get(
												(int) ydummy);
										distArray += weightSim[k] * simval;
										simVals.append(Float.toString(simval)
												+ " ");
									} else {
										distArray += 0.0;
										simVals.append(Float.toString(0.0f)
												+ " ");
									}
									break;
								}
							}
						}

						float[] x_skill = new float[dimension - 6];
						float[] y_skill = new float[dimension - 6];
						x_skill = Arrays.copyOfRange(x, 6, dimension);
						y_skill = Arrays.copyOfRange(y, 6, dimension);

						x = null;

						simval = (float) Math
								.pow(distance(x_skill, y_skill), 4);
						distArray += weightSim[6] * simval;
						simVals.append(Float.toString(simval));

						ListElem ne = new ListElem(dimension, distArray, id2,
								simVals.toString());

						simVals = null;

						PriorityQueue<ListElem> knnQueue = knnQueueR.get(i);
						knnQueue.add(ne);
						if (knnQueue.size() > knn)
							knnQueue.poll();
					} // [0 . . number - 1]
				} // while - inner

				brForS.close();
				frForS.close();

				for (int j = 0; j < number; j++) {
					PriorityQueue<ListElem> knnQueue = knnQueueR.get(j);
					int id1 = idR[j];
					for (int i = 0; i < knn; i++) {
						ListElem e = knnQueue.poll();
						try {
							output.collect(
									NullWritable.get(),
									new Text(id1 + " "
											+ Integer.toString(e.getId()) + " "
											+ Float.toString(e.getDist()) + " "
											+ e.getSimVals()));
						} catch (NullPointerException e1) {
							break;
						}
					} // for
				}
				System.out
						.println("Time Taken By Block: "
								+ Long.toString((System.currentTimeMillis() - t3) / 1000));
				reporter.progress();
			} // while - outer
			brForR.close();
			frForR.close();
			System.out.println("Total Time Taken By Reducer: "
					+ Long.toString((System.currentTimeMillis() - t1) / 1000));
			// System.gc();
		} // reduce

		public void close() throws IOException {
			// mos.close();
		}

	} // Reducer

	static int printUsage() {
		System.out
				.println("NPhase1 [-m <maps>] [-r <reduces>] [-p <numberOfPartitions>] "
						+ "[-d <dimension>] [-k <knn>] [-b <blockSize(#records) for R>] "
						+ "<input (R)> <input (S)> <output>");
		System.out
				.println("Check if Weights are between 0 & 1 AND Sum of Weights < 1");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * The main driver for H-BNLJ program. Invoke this method to submit the
	 * map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */
	public int run(String[] args) throws Exception {
		int numberOfPartition = 2;
		String weightSim = "";
		// boolean self_join = false;
		JobConf conf = new JobConf(getConf(), NPhase1_v01.class);
		conf.setJobName("NPhase1");
		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(NPhase1Value.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					// conf.setNumMapTasks(Integer.parseInt(args[++i]));
					++i;
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else if ("-p".equals(args[i])) {
					numberOfPartition = Integer.parseInt(args[++i]);
					conf.setInt("numberOfPartition", numberOfPartition);
				} else if ("-d".equals(args[i])) {
					conf.setInt("dimension", Integer.parseInt(args[++i]));
				} else if ("-k".equals(args[i])) {
					conf.setInt("knn", Integer.parseInt(args[++i]));
				} else if ("-b".equals(args[i])) {
					conf.setInt("blockSize", Integer.parseInt(args[++i]));
					/*
					 * } else if ("-sj".equals(args[i])) { self_join =
					 * Boolean.parseBoolean(args[++i]); conf.set("self_join",
					 * Boolean.toString(self_join));
					 */} else {
					other_args.add(args[i]);
				}
				// set the number of reducers
				conf.setNumReduceTasks(numberOfPartition * numberOfPartition);
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "
						+ args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "
						+ args[i - 1]);
				return printUsage();
			}
		}

		if (other_args.size() != 11) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 11.");
			return printUsage();
		}

		DistributedCache.addCacheFile(new URI(other_args.get(0)
				+ "ind_ind_sim.ser"), conf);
		DistributedCache.addCacheFile(new URI(other_args.get(0)
				+ "subfa_subfa_sim.ser"), conf);
		DistributedCache.addCacheFile(new URI(other_args.get(0)
				+ "insti_insti_sim.ser"), conf);
		DistributedCache.addCacheFile(new URI(other_args.get(0)
				+ "jt_jt_sim.ser"), conf);

		FileInputFormat.setInputPaths(conf, other_args.get(1));
		// System.out.println("set R to  the input path");
		FileInputFormat.addInputPaths(conf, other_args.get(2));
		// final String NAME_NODE = "hdfs://";
		// DistributedCache.addCacheFile(new URI(NAME_NODE
		// + "/user/cloudera/similarprofile/Input/ind_ind_sim.ser"), conf);
		// System.out.println("set S to  the input path");
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(3)));
		int st = 4;
		float weightSum = 0.0f;

		for (int i = 0; i < 7; i++) {
			weightSim += other_args.get(st + i) + " ";
			float weightTest = Float.parseFloat(other_args.get(st + i));
			weightSum += weightTest;
			if (weightTest < 0 || weightTest > 1)
				return printUsage();
		}
		if (weightSum != 1)
			return printUsage();

		conf.set("weightSim", weightSim);
		JobClient.runJob(conf);

		return 0;
	} // run

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NPhase1_v01(), args);
		System.exit(res);
	}
} // NPhase1