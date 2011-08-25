package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Tag implements Writable {

	private Text v = new Text();
	private Text k = new Text();

	public Text getV() {
		return v;
	}

	public void setV(Text v) {
		this.v = v;
	}

	public Text getK() {
		return k;
	}

	public void setK(Text k) {
		this.k = k;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		k.write(out);
		v.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		k.readFields(in);
		v.readFields(in);
	}
	
	@Override
	public String toString() {
		return "Tag(" + k + "=" + v + ")";
	}
}
