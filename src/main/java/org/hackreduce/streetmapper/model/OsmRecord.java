package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class OsmRecord implements Writable {

	private LongWritable id = new LongWritable(0);

	public OsmRecord() {}
	
	public OsmRecord(Text xmlFilename, Text xml) {
		// TODO parse XML
	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
	}

}
