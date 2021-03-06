package org.htmedia.shine.simprofileknn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * Phase1 of Similar Candidate Block Nested Loop KNN Method.
 */
public class NPhase1NewProfilesSimilarityUpdate extends Configured implements
		Tool {

	public static final Log LOG = LogFactory
			.getLog(NPhase1NewProfilesSimilarityUpdate.class);

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, NPhase1Value> {
		// public static final Log LOG = LogFactory.getLog(MapClass.class);
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

		/**
		 * Configuring Mapper properties
		 */
		public void configure(JobConf job) {
			System.out.println("Map Task configuration started ....");
			LOG.info("Map Task configuration started ....");
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
				LOG.info("Invalid input file source@NPhase1");
				System.exit(-1);
			}
			System.out.println("Map Task configured ...");
			LOG.info("Map Task configured ...");
		}

		/**
		 * Map Task to Partition the input data sets (R and S) into multiple
		 * buckets.
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, NPhase1Value> output,
				Reporter reporter) throws IOException {
			String[] parts = value.toString().split("[,]|[|]");
			String recId = parts[recIdOffset];
			try {
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
						System.out
								.println("The record comes from unknow file!!!");
						LOG.info("The record comes from unknow file!!!");
						System.exit(-1);
					}

					IntWritable mapKey = new IntWritable(groupID);
					output.collect(mapKey, np1v);
				}
			} catch (Exception e) {
				System.out.println("Error for id : " + recId + " in file Id : "
						+ Integer.toString(fileId));
				LOG.info("Error for id : " + recId + " in file Id : "
						+ Integer.toString(fileId));
				e.printStackTrace();
			}
		} // map
	} // MapClass

	/**
	 * Reduce Task to perform Block wise Nested Loop join for records in the
	 * same partition/bucket.
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, NPhase1Value, NullWritable, Text> {
		// public static final Log LOG = LogFactory.getLog(Reduce.class);
		private int bufferSize = 1024 * 1024;
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
		private float skillCutoff;
		private float minProfileCutoff;
		private float weightedCutoff;
		private float skillCSPower;
		private float skillJacardWeight;
		private int knn;
		private float[] weightSim = new float[] { 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
				0.0f, 0.0f };
		private Path[] localFiles;
		private HashMap<Integer, HashMap<Integer, Float>> indMap;
		private HashMap<Integer, HashMap<Integer, Float>> subFAMap;
		private HashMap<Integer, HashMap<Integer, Float>> instiMap;
		private HashMap<Integer, HashMap<Integer, HashMap<Integer, Float>>> jtMap;
		private HashMap<Integer, HashSet<Integer>> mapSModify;
		private HashMap<Integer, Integer> mapROSMin;

		private int binSize = 5;
		private Configuration jobinfo;

		public void configure(JobConf job) {
			System.out.println("Reduce Task configuration started ...");
			LOG.info("Reduce Task configuration started ...");
			numberOfPartition = job.getInt("numberOfPatition", 2);
			bufferSize = bufferSize * job.getInt("bufferSize", 8);
			dimension = job.getInt("dimension", 1);
			blockSize = job.getInt("blockSize", 1024);
			skillCutoff = job.getFloat("skillCutoff", 0.0f);
			weightedCutoff = job.getFloat("weightedCutoff", 0.0f);
			knn = job.getInt("knn", 1024);
			skillJacardWeight = job.getFloat("skillJacardWeight", 0.0f);
			skillCSPower = job.getFloat("skillCSPower", 0.0f);
			minProfileCutoff = 0.0f;

			/**
			 * For Distributed Cache File for accessing Serialized files from s3
			 * location
			 */
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
			System.out.println("Reduce Task Configured ...");
			LOG.info("Reduce Task Configured ...");
		}

		/**
		 * Deserialize distributed Cache file -- Ind, sFA, Edu
		 */
		public static HashMap<Integer, HashMap<Integer, Float>> itemDeSerialization(
				String filePath) {
			long t1 = System.currentTimeMillis();
			HashMap<Integer, HashMap<Integer, Float>> map = null;
			try {
				FileInputStream fin = new FileInputStream(filePath);
				ObjectInputStream ois = new ObjectInputStream(fin);
				map = (HashMap<Integer, HashMap<Integer, Float>>) ois
						.readObject();
				System.out
						.println("Time Taken to DeSerialize the Object : "
								+ Long.toString((System.currentTimeMillis() - t1) / 1000));
				LOG.info("Time Taken to DeSerialize the Object : "
						+ Long.toString((System.currentTimeMillis() - t1) / 1000));
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

		/**
		 * Deserialize distributed Cache file -- JT FA Wise
		 */
		public static HashMap<Integer, HashMap<Integer, HashMap<Integer, Float>>> itemDeSerializationFAWise(
				String filePath) {
			long t1 = System.currentTimeMillis();
			HashMap<Integer, HashMap<Integer, HashMap<Integer, Float>>> map = null;
			try {
				FileInputStream fin = new FileInputStream(filePath);
				ObjectInputStream ois = new ObjectInputStream(fin);
				map = (HashMap<Integer, HashMap<Integer, HashMap<Integer, Float>>>) ois
						.readObject();
				System.out
						.println("Time Taken to DeSerialize the Object : "
								+ Long.toString((System.currentTimeMillis() - t1) / 1000));
				LOG.info("Time Taken to DeSerialize the Object : "
						+ Long.toString((System.currentTimeMillis() - t1) / 1000));
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

		public static HashMap<Integer, HashSet<Integer>> user2MapFromCSV(
				String filePath) {
			String line = "";
			HashMap<Integer, HashSet<Integer>> itemMap = new HashMap<Integer, HashSet<Integer>>();
			File dataFile = new File(filePath);
			int u2;
			try {
				BufferedReader reader = new BufferedReader(new FileReader(
						dataFile));
				try {
					while ((line = reader.readLine()) != null) {
						String[] lineElements = line.split(",");
						u2 = Integer.parseInt(lineElements[0]);
						HashSet<Integer> u1Set = new HashSet<Integer>(50);
						for (int i = 1; i < lineElements.length; i++) {
							u1Set.add(Integer.parseInt(lineElements[i]));
						}
						itemMap.put(u2, u1Set);
					}
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println("Error while making hashmap");
					System.exit(0);
				}
			} catch (FileNotFoundException e1) {
				System.err.println("Input File does not exist");
				System.exit(0);
				e1.printStackTrace();
			}
			return itemMap;
		}

		public static HashMap<Integer, Integer> userOSMapFromCSV(String filePath) {
			String line = "";
			HashMap<Integer, Integer> itemMap = new HashMap<Integer, Integer>();
			File dataFile = new File(filePath);
			int u1;
			int os;
			try {
				BufferedReader reader = new BufferedReader(new FileReader(
						dataFile));
				try {
					while ((line = reader.readLine()) != null) {
						String[] lineElements = line.split(",");
						u1 = Integer.parseInt(lineElements[0]);
						os = Integer.parseInt(lineElements[1]);
						itemMap.put(u1, os);
					}
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println("Error while making hashmap");
					System.exit(0);
				}
			} catch (FileNotFoundException e1) {
				System.err.println("Input File does not exist");
				System.exit(0);
				e1.printStackTrace();
			}
			return itemMap;
		}

		/**
		 * Cosine Distance Method for Skills
		 */
		public static float distance(float[] p1, float[] p2, float skillCutoff) {
			float dotProduct = 0.0f;
			float lengthSquaredp1 = 0.0f;
			float lengthSquaredp2 = 0.0f;
			for (int i = 0; i < p1.length; i++) {
				if (p1[i] > skillCutoff) { // Distance Measure only if
											// similarity is above skillCutoff
					lengthSquaredp1 += p1[i] * p1[i];
					lengthSquaredp2 += p2[i] * p2[i];
					dotProduct += p1[i] * p2[i];
				}
			}
			float denominator = (float) (Math.sqrt(lengthSquaredp1) * Math
					.sqrt(lengthSquaredp2));
			// correct for floating-point rounding errors
			if (denominator < dotProduct) {
				denominator = dotProduct;
			}
			// correct for zero-vector corner case
			if (denominator == 0 && dotProduct == 0) {
				return 0;
			}
			return dotProduct / denominator;
		}

		/**
		 * Similarity weightage function Experience wise
		 */
		public static float[] expWiseWeight(float exp_years) {
			float[] weight = new float[7];
			float sumWeight = 0;
			weight[0] = (float) (0.05 + 0.1 / (1 + Math.exp(-1
					* (0.75 * Math.exp(0.23 * exp_years) - 3)))); // bounded
																	// function
																	// between
																	// 5% and
																	// 15%
			weight[1] = (float) (0.05 + 0.05 / (1 + Math.exp(-1
					* (0.75 * Math.exp(0.23 * exp_years) - 3)))); // bounded
																	// function
																	// between
																	// 5% and
																	// 10%
			weight[2] = (float) (0.05 + 0.15 / (1 + Math.exp(-1
					* (0.55 * Math.exp(0.2 * exp_years) - 3)))); // bounded
																	// function
																	// between
																	// 5% and
																	// 20%
			weight[3] = (float) (0.15 + 0.10 / (1 + Math.exp(-1
					* (0.55 * Math.exp(0.15 * exp_years) - 3)))); // bounded
																	// function
																	// between
																	// 15% and
																	// 25%
			weight[4] = (float) (0.05 + 0.1 / (1 + Math.exp(1 * (0.85 * Math
					.exp(0.3 * exp_years) - 3)))); // bunded function between 5%
													// and 15%
			weight[5] = (float) (0.14 + 0.02 / (1 + Math.exp(1 * (0.45 * Math
					.exp(0.1 * exp_years) - 3)))); // bounded function between
													// 10% and 15%
			weight[6] = (float) (0.30 + 0.10 / (1 + Math.exp(1 * (0.55 * Math
					.exp(0.2 * exp_years) - 3)))); // bounded function between
													// 30% and 40%
			for (int i = 0; i < 7; i++) {
				sumWeight += weight[i];
			}
			for (int i = 0; i < 7; i++) {
				weight[i] = weight[i] / sumWeight;
			}
			return weight;
		}

		/**
		 * Reduce working on each outer block and evaluating similarity with
		 * each elements in inner block
		 */
		public void reduce(IntWritable key, Iterator<NPhase1Value> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			System.out.println("Reducer started ...");
			LOG.info("Reducer started ...");
			String algorithm = "nested_loop";
			String prefix_dir = algorithm + "-"
					+ Integer.toString(numberOfPartition) + "-"
					+ key.toString();
			long t1 = System.currentTimeMillis();

			/**
			 * Deserializing Similarity objets
			 */
			indMap = itemDeSerialization(localFiles[0].toString());
			subFAMap = itemDeSerialization(localFiles[1].toString());
			instiMap = itemDeSerialization(localFiles[2].toString());
			jtMap = itemDeSerializationFAWise(localFiles[3].toString());
			mapROSMin = userOSMapFromCSV(localFiles[4].toString());

			// indMap =
			// itemDeSerialization("/home/cloudera/Documents/similarprofiles/Input/ind_ind_sim.ser");
			// subFAMap =
			// itemDeSerialization("/home/cloudera/Documents/similarprofiles/Input/subfa_subfa_sim.ser");
			// instiMap =
			// itemDeSerialization("/home/cloudera/Documents/similarprofiles/Input/insti_insti_sim.ser");
			// jtMap =
			// itemDeSerializationFAWise("/home/cloudera/Documents/similarprofiles/Input/jt_jt_sim.ser");
			// mapROSMin =
			// userOSMapFromCSV("/home/cloudera/Documents/similarprofiles/Input/u1_min_os.csv");

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
				// Spilling of Records to local disk. Each Reducer will dump its
				// record
				// Value format <rid, coord, src>
				NPhase1Value np1v = values.next();
				String record = np1v.getFirst().toString();
				StringBuilder recordbuilder = new StringBuilder(5 * dimension);
				recordbuilder.append(record);
				String[] partsSecond = np1v.getSecond().toStrings();
				String[] partsThird = np1v.getThird().toStrings();
				int srcId = (int) np1v.getFourth().get();

				try {
					for (int i = 0; i < binSize; i++)
						recordbuilder.append("," + partsSecond[i]);
					for (int i = 0; i < dimension; i++)
						recordbuilder.append("," + partsThird[i]);
					if (srcId == 0) {
						bwForR.write(recordbuilder.toString() + "\n");
					} else if (srcId == 1) {
						bwForS.write(recordbuilder.toString() + "\n");
					} else {
						System.out.println("unknown file number");
						LOG.info("unknown file number");
						System.exit(-1);
					}
				} catch (Exception e) {
					System.out.println("Error for record : " + record
							+ " while spilling records");
					LOG.info("Error for record : " + record
							+ " while spilling records");
					e.printStackTrace();
				}

			}

			System.out.println("Time Taken By Reducer to Spill Records: "
					+ Long.toString((System.currentTimeMillis() - t2) / 1000));
			LOG.info("Time Taken By Reducer to Spill Records: "
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
			int metrics = 7;
			int[] idR = new int[number];
			String[][] binR = new String[number][binSize];
			float[][] coordR = new float[number][dimension];
			float[][] weightSimR = new float[number][metrics];
			float[] minProfileCutoffR = new float[number];
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

					String parts[] = line.split(",");
					int id1 = Integer.parseInt(parts[0]);
					idR[ii] = id1;
					try {
						int st = 1;
						String[] x_bin = binR[ii];
						for (int i = 0; i < binSize; i++)
							x_bin[i] = parts[st + i];
						st = binSize + 1;
						float[] x = coordR[ii];
						for (int i = 0; i < dimension; i++)
							x[i] = Float.parseFloat(parts[st + i]);

						if (x[0] != -1) {
							weightSim = expWiseWeight(x[0]);
							System.out.println();
						} else {
							weightSim = expWiseWeight(0);
						}
						minProfileCutoff = 0.0f;

						for (int weight = 0; weight < 6; weight++) {
							if (x[weight] == -1)
								continue;
							minProfileCutoff += weightSim[weight];
						}
						minProfileCutoff = weightedCutoff * minProfileCutoff;
						weightSimR[ii] = weightSim;
						minProfileCutoffR[ii] = minProfileCutoff;
					} catch (Exception e) {
						System.out.println("Error for id : "
								+ Integer.toString(id1)
								+ " while reading a block of R");
						LOG.info("Error for id : " + Integer.toString(id1)
								+ " while reading a block of R");
						e.printStackTrace();
					}

				}

				// knn-join with S
				FileReader frForS = new FileReader(innerTable);
				BufferedReader brForS = new BufferedReader(frForS, bufferSize);

				while (true) {
					String line = brForS.readLine();
					if (line == null)
						break;
					String parts[] = line.split(",");
					int id2 = Integer.parseInt(parts[0]);
					try {
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

							weightSim = weightSimR[i];
							minProfileCutoff = minProfileCutoffR[i];
							if (y_bin[2].indexOf(x_bin[1]) == -1)
								continue;

							if (y_bin[4].indexOf(x_bin[3]) == -1)
								continue;
							StringBuilder simVals = new StringBuilder(250);
							simVals.append(y_bin[0] + ",");
							// if (x[0] != y[0]) // To handle bin similarity
							// continue;
							for (int k = 0; k < 6; k++) {
								if (x[k] == -1 || y[k] == -1) {
									distArray += 0.0;
									simVals.append(Float.toString(0.0f) + ",");
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
											simval = (float) Math.exp(-2
													* dummy);
											distArray += weightSim[k] * simval;
											simVals.append(Float
													.toString(simval) + ",");
										} else {
											if (xdummy > ydummy) {
												dummy = ydummy;
												ydummy = xdummy;
												xdummy = dummy;
											}
											factor = (ydummy - xdummy)
													/ (xdummy);
											simval = (float) Math.exp(-1 * 0.4
													* (Math.pow(factor, 2))
													* xdummy);
											distArray += weightSim[k] * simval;
											simVals.append(Float
													.toString(simval) + ",");
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
											simVals.append(Float
													.toString(simval) + ",");
										} else {
											if (xdummy > ydummy) {
												dummy = ydummy;
												ydummy = xdummy;
												xdummy = dummy;
											}
											factor = (ydummy - xdummy)
													/ (xdummy);
											simval = (float) Math
													.exp(-1
															* 1.3
															* (0.2 * Math.pow(
																	factor, 4) + 0.2 * Math
																	.pow(factor,
																			2))
															* xdummy);
											distArray += weightSim[k] * simval;
											simVals.append(Float
													.toString(simval) + ",");
										}
										break;
									case 2:
										if (xdummy > ydummy) {
											dummy = ydummy;
											ydummy = xdummy;
											xdummy = dummy;
										}

										if (indMap.get((int) xdummy)
												.containsKey((int) ydummy)) {
											simval = indMap.get((int) xdummy)
													.get((int) ydummy);
											distArray += weightSim[k] * simval;
											simVals.append(Float
													.toString(simval) + ",");
										} else {
											distArray += 0.0;
											simVals.append(Float.toString(0.0f)
													+ ",");
										}
										break;
									case 3:
										if (xdummy > ydummy) {
											dummy = ydummy;
											ydummy = xdummy;
											xdummy = dummy;
										}
										if (subFAMap.get((int) xdummy)
												.containsKey((int) ydummy)) {
											simval = subFAMap.get((int) xdummy)
													.get((int) ydummy);
											distArray += weightSim[k] * simval;
											simVals.append(Float
													.toString(simval) + ",");
										} else {
											distArray += 0.0;
											simVals.append(Float.toString(0.0f)
													+ ",");
										}
										break;
									case 4:
										if (xdummy > ydummy) {
											dummy = ydummy;
											ydummy = xdummy;
											xdummy = dummy;
										}
										if (instiMap.get((int) xdummy)
												.containsKey((int) ydummy)) {
											simval = instiMap.get((int) xdummy)
													.get((int) ydummy);
											distArray += weightSim[k] * simval;
											simVals.append(Float
													.toString(simval) + ",");
										} else {
											distArray += 0.0;
											simVals.append(Float.toString(0.0f)
													+ ",");
										}
										break;
									case 5:
										if (xdummy > ydummy) {
											dummy = ydummy;
											ydummy = xdummy;
											xdummy = dummy;
										}
										try {
											if (jtMap.containsKey(Integer
													.parseInt(x_bin[0]))) {
												if (jtMap
														.get(Integer
																.parseInt(x_bin[0]))
														.containsKey(
																(int) xdummy)) {
													if (jtMap
															.get(Integer
																	.parseInt(x_bin[0]))
															.get((int) xdummy)
															.containsKey(
																	(int) ydummy)) {
														simval = jtMap
																.get(Integer
																		.parseInt(y_bin[0]))
																.get((int) xdummy)
																.get((int) ydummy);
														distArray += weightSim[k]
																* simval;
														simVals.append(Float
																.toString(simval)
																+ ",");
													} else {
														distArray += 0.0;
														simVals.append(Float
																.toString(0.0f)
																+ ",");
													}
												} else {
													distArray += 0.0;
													simVals.append(Float
															.toString(0.0f)
															+ ",");
												}
											} else {
												distArray += 0.0;
												simVals.append(Float
														.toString(0.0f) + ",");
											}
										} catch (Exception e) {
											System.out.println(Integer
													.parseInt(y_bin[0])
													+ ","
													+ (int) xdummy
													+ ","
													+ (int) ydummy);
											distArray += 0.0;
											simVals.append(Float.toString(0.0f)
													+ ",");
										}
										break;
									}
								}
							}

							if (distArray < minProfileCutoff) {
								continue;
							}

							float dotProduct = 0.0f;
							float lengthSquaredp1 = 0.0f;
							float lengthSquaredp2 = 0.0f;
							int jacardUnion = 0;
							int jacardIntersection = 0;
							float simvalCosine = 0.0f;
							float simvalJacard = 0.0f;
							int count = 0;
							for (int j = 6; j < x.length; j++) {
								if (x[j] > skillCutoff) { // Distance Measure
															// only
															// if similarity is
															// above skillCutoff
									lengthSquaredp1 += x[j] * x[j];
									lengthSquaredp2 += y[j] * y[j];
									dotProduct += x[j] * y[j];
									count++;
								}
								if (x[j] == 1 || y[j] == 1) {
									jacardUnion++;
								}
								if (x[j] == 1 && y[j] == 1) {
									jacardIntersection++;
								}
							}

							float denominator = (float) (Math
									.sqrt(lengthSquaredp1) * Math
									.sqrt(lengthSquaredp2));

							// correct for floating-point rounding errors
							if (denominator < dotProduct) {
								denominator = dotProduct;
							}

							// correct for zero-vector corner case
							if (denominator == 0 && dotProduct == 0) {
								simvalCosine = 0.0f;
							} else if (count == 1) {
								simvalCosine = dotProduct;
							} else {
								simvalCosine = dotProduct / denominator;
							}

							simvalCosine = (float) Math.pow(simvalCosine,
									skillCSPower);

							if (jacardIntersection == 0 || jacardUnion == 0) {
								simvalJacard = 0.0f;
							} else {
								simvalJacard = (float) jacardIntersection
										/ jacardUnion;
							}

							simval = (((1 - skillJacardWeight) * simvalCosine) + (skillJacardWeight * simvalJacard));
							distArray += weightSim[6] * simval;
							simVals.append(Float.toString(simval));

							try {
								if (((float) mapROSMin.get(idR[i])) / 1000 > distArray)
									continue;
							} catch (Exception e) {
								System.out
										.println("Error while fetching id :"
												+ Integer.toString(id2)
												+ " for Min Overall Similarity during modification check");
								LOG.info("Error while fetching id :"
										+ Integer.toString(id2)
										+ " for Min Overall Similarity during modification check");
								continue;
							}

							ListElem ne = new ListElem(dimension, distArray,
									id2, simVals.toString());

							simVals = null;

							PriorityQueue<ListElem> knnQueue = knnQueueR.get(i);
							knnQueue.add(ne);
							if (knnQueue.size() > knn)
								knnQueue.poll();
						} // [0 . . number - 1]
					} catch (Exception e) {
						System.out.println("Error for id : "
								+ Integer.toString(id2)
								+ " while reading a block of R");
						LOG.info("Error for id : " + Integer.toString(id2)
								+ " while reading a block of R");
						e.printStackTrace();
					}
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
									new Text(id1 + ","
											+ Integer.toString(e.getId()) + ","
											+ Float.toString(e.getDist()) + ","
											+ e.getSimVals()));
						} catch (NullPointerException e1) {
							break;
						}
					} // for
				}
				System.out
						.println("Time Taken By Block: "
								+ Long.toString((System.currentTimeMillis() - t3) / 1000));
				LOG.info("Time Taken By Block: "
						+ Long.toString((System.currentTimeMillis() - t3) / 1000));
				reporter.progress();

			} // while - outer
			brForR.close();
			frForR.close();
			System.out.println("Reduce Task Completed ...");
			LOG.info("Reduce Task Completed ...");
			System.out.println("Total Time Taken By Reducer: "
					+ Long.toString((System.currentTimeMillis() - t1) / 1000));
			LOG.info("Total Time Taken By Reducer: "
					+ Long.toString((System.currentTimeMillis() - t1) / 1000));
			// System.gc();
		} // reduce

		public void close() throws IOException {
			// mos.close();
		}

	} // Reducer

	/*
	 * Prompt in case of error in Input arguments
	 */
	static int printUsage() {
		System.out
				.println("SimUpdNPhase1 [-map <maps>] [-red <reduces>] [-part <numberOfPartitions>] "
						+ "[-dim <dimension>] [-knn <knn>] [-wc <weightedCutoff>] [-sc <skillCutoff>] "
						+ "[-sjw <skillJacardWeight>] [-scsp <skillCSPower>] [-buffer <bufferSize>]"
						+ "[-block <blockSize(#records) for R>] <distributed cache location (ser)> <input (R)> <input (S)> <output>");
		LOG.info("NPhase1 [-map <maps>] [-red <reduces>] [-part <numberOfPartitions>] "
				+ "[-dim <dimension>] [-knn <knn>] [-wc <weightedCutoff>] [-sc <skillCutoff>] "
				+ "[-sjw <skillJacardWeight>] [-scsp <skillCSPower>] [-buffer <bufferSize>]"
				+ "[-block <blockSize(#records) for R>] <distributed cache location (ser)> <input (R)> <input (S)> <output>");
		System.out
				.println("Check if Weights are between 0 & 1 AND Sum of Weights < 1");

		LOG.info("Check if Weights are between 0 & 1 AND Sum of Weights < 1");
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
		int numberOfPartition = 2;
		String weightSim = "";
		JobConf conf = new JobConf(getConf(),
				NPhase1NewProfilesSimilarityUpdate.class);
		conf.setJobName("NPhase1NewProfilesSimilarityUpdate");
		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(NPhase1Value.class);

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
					conf.setInt("knn", Integer.parseInt(args[++i]));
				} else if ("-wc".equals(args[i])) {
					conf.setFloat("weightedCutoff", Float.parseFloat(args[++i]));
				} else if ("-sc".equals(args[i])) {
					conf.setFloat("skillCutoff", Float.parseFloat(args[++i]));
				} else if ("-sjw".equals(args[i])) {
					conf.setFloat("skillJacardWeight",
							Float.parseFloat(args[++i]));
				} else if ("-scsp".equals(args[i])) {
					conf.setFloat("skillCSPower", Float.parseFloat(args[++i]));
				} else if ("-buffer".equals(args[i])) {
					conf.setFloat("bufferSize", Integer.parseInt(args[++i]));
				} else if ("-block".equals(args[i])) {
					conf.setInt("blockSize", Integer.parseInt(args[++i]));
				} else {
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
				LOG.info("ERROR: Required parameter missing from "
						+ args[i - 1]);
				return printUsage();
			}
		}

		if (other_args.size() != 4) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 11.");
			LOG.info("ERROR: Wrong number of parameters: " + other_args.size()
					+ " instead of 11.");
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
		DistributedCache.addCacheFile(new URI(other_args.get(0)
				+ "u1_min_os.csv"), conf);

		// System.out.println("set R to  the input path");
		FileInputFormat.setInputPaths(conf, other_args.get(1));
		// System.out.println("set S to  the input path");
		FileInputFormat.addInputPaths(conf, other_args.get(2));

		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(3)));

		JobClient.runJob(conf);

		return 0;
	} // run

	public static void main(String[] args) throws Exception {
		System.out
				.println("NPhase1NewProfilesSimilarityUpdate Run Started ...");
		LOG.info("NPhase1NewProfilesSimilarityUpdate Run Started ...");
		int res = ToolRunner.run(new Configuration(),
				new NPhase1NewProfilesSimilarityUpdate(), args);
		System.exit(res);
	}
} // NPhase1