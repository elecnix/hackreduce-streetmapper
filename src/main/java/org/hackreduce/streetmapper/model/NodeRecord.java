package org.hackreduce.streetmapper.model;

import org.apache.hadoop.io.FloatWritable;

public class NodeRecord extends OsmRecord {

	private FloatWritable lat = new FloatWritable(0);
	private FloatWritable lon = new FloatWritable(0);
	
	public NodeRecord() {}

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
	
}
