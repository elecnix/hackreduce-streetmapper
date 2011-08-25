package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class NodeRef implements Writable {
	
	private LongWritable ref = new LongWritable(0);

	public LongWritable getRef() {
		return ref;
	}

	public void setRef(LongWritable ref) {
		this.ref = ref;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		ref.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ref.readFields(in);
	}

}
