package org.htmedia.shine.simprofileknn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class NPhase1Value implements WritableComparable<NPhase1Value> {

	private IntWritable first;
	private ArrayWritable second;
	private ArrayWritable third;
	private ByteWritable fourth;
	private int dim;

	public NPhase1Value() {
		this.first = new IntWritable();
		this.second = new ArrayWritable(new String[]{"", "", "", "", "", ""});
		this.third = new ArrayWritable(FloatWritable.class);
		this.fourth = new ByteWritable();
		this.dim = 0;
	}

	public NPhase1Value(int first, String[] second, float[] third, byte fourth,
			int dimension) {
		set(new IntWritable(first), second, third, new ByteWritable(fourth),
				dimension);
	}

	public void set(IntWritable first, String[] second, float[] third,
			ByteWritable fourth, int dimension) {
		this.first = first;
		this.fourth = fourth;
		this.dim = dimension;
		String[] stringArray = new String[5];
		for (int i = 0; i < 5; i++)
			stringArray[i] = new String(second[i]);
		this.second = new ArrayWritable(stringArray);

		FloatWritable[] floatArray = new FloatWritable[dimension];
		for (int i = 0; i < dimension; i++)
			floatArray[i] = new FloatWritable(third[i]);
		this.third = new ArrayWritable(FloatWritable.class, floatArray);
	}

	public IntWritable getFirst() {
		return first;
	}

	public ArrayWritable getSecond() {
		return second;
	}

	public ArrayWritable getThird() {
		return third;
	}

	public ByteWritable getFourth() {
		return fourth;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		third.write(out);
		fourth.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		third.readFields(in);
		fourth.readFields(in);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof NPhase1Value) {
			NPhase1Value np1v = (NPhase1Value) o;
			return first.equals(np1v.first) && second.equals(np1v.second)
					&& third.equals(np1v.third) && fourth.equals(np1v.fourth);
		}
		return false;
	}

	@Override
	public String toString() {
		int dimension = dim;
		String result;
		result = first.toString() + " ";
		String[] secondParts = second.toStrings();
		for (int i = 0; i < 5; i++)
			result = result + secondParts[i] + " ";
		String[] thirdParts = third.toStrings();
		for (int i = 0; i < dimension; i++)
			result = result + thirdParts[i] + " ";
		return result + fourth.toString();
	}

	public String toString(int dimension) {
		String result;
		result = first.toString() + " ";
		String[] secondParts = second.toStrings();
		for (int i = 0; i < 5; i++)
			result = result + secondParts[i] + " ";
		String[] thirdParts = third.toStrings();
		for (int i = 0; i < dimension; i++)
			result = result + thirdParts[i] + " ";
		return result + fourth.toString();
	}

	@Override
	public int compareTo(NPhase1Value np1v) {
		return 1;
	}

}
