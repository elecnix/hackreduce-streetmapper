package org.hackreduce.streetmapper.model;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

public class WayId extends LongWritable {

	public WayId() {
	}

	public WayId(long value) {
		super(value);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		if (get() == 0) throw new IOException("Non-initialized");
		super.write(out);
	}
}
