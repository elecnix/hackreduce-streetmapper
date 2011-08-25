package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class OsmRecord implements Writable {

	private LongWritable id = new LongWritable(0);
	private LongWritable ref = new LongWritable(0);
	private LongWritable uid = new LongWritable(0);
	private Text timestamp = new Text();
	private LongWritable changeset = new LongWritable(0);
	private Text user = new Text();
	private Text v = new Text();
	private Text k = new Text();
	private IntWritable version = new IntWritable(0);
	private BooleanWritable visible = new BooleanWritable(true);

	private static Logger LOG = Logger.getLogger(OsmRecord.class.getName());
	
	public OsmRecord() {}
	
	public LongWritable getId() {
		return id;
	}

	public void setId(LongWritable id) {
		this.id = id;
	}

	public LongWritable getRef() {
		return ref;
	}

	public void setRef(LongWritable ref) {
		this.ref = ref;
	}

	public LongWritable getUid() {
		return uid;
	}

	public void setUid(LongWritable uid) {
		this.uid = uid;
	}

	public Text getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Text timestamp) {
		this.timestamp = timestamp;
	}

	public LongWritable getChangeset() {
		return changeset;
	}

	public void setChangeset(LongWritable changeset) {
		this.changeset = changeset;
	}

	public Text getUser() {
		return user;
	}

	public void setUser(Text user) {
		this.user = user;
	}

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

	public IntWritable getVersion() {
		return version;
	}

	public void setVersion(IntWritable version) {
		this.version = version;
	}

	public BooleanWritable getVisible() {
		return visible;
	}

	public void setVisible(BooleanWritable visible) {
		this.visible = visible;
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
