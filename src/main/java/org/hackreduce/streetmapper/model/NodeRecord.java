package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;

public class NodeRecord extends OsmRecord {

	private NodeId id = new NodeId(0);
	private FloatWritable lat = new FloatWritable(0);
	private FloatWritable lon = new FloatWritable(0);
	
	public NodeRecord() {}
	
	public NodeId getId() {
		return id;
	}

	public void setId(NodeId id) {
		this.id = id;
	}

	public FloatWritable getLat() {
		return lat;
	}

	public void setLat(FloatWritable lat) {
		this.lat = lat;
	}

	public FloatWritable getLon() {
		return lon;
	}

	public void setLon(FloatWritable lon) {
		this.lon = lon;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		id.write(out);
		lat.write(out);
		lon.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		id.readFields(in);
		lat.readFields(in);
		lon.readFields(in);
	}
}
