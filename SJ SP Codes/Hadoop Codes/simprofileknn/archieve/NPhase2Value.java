
package org.htmedia.shine.simprofileknn.archieve;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class NPhase2Value implements WritableComparable<NPhase2Value> {

	private IntWritable first;
	private IntWritable second;
	private Text third; 

	public NPhase2Value() {
		set(new IntWritable(), new IntWritable(), new Text());
	}

	public NPhase2Value(int first, int second, Text third) {
		set(new IntWritable(first), new IntWritable(second), new Text(third));
	}

	public void set(IntWritable first, IntWritable second, Text third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}

	public IntWritable getFirst() {
		return first;
	}

	public IntWritable getSecond() {
		return second;
	}

	public Text getThird() {
		return third;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		third.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		third.readFields(in);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof NPhase2Value) {
			NPhase2Value np2v = (NPhase2Value) o;
			return first.equals(np2v.first) && second.equals(np2v.second) && third.equals(np2v.third);
		}
		return false;
	}

	@Override
	public String toString() {
		return first.toString() + "," + second.toString() + "," + third;
	}

	@Override
	public int compareTo(NPhase2Value np2v) {
		return 1;
	}

}
