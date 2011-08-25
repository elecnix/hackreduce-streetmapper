package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.MapWritable;

public class WayRecord extends OsmRecord {
	
	private MapWritable nodeRefs = new MapWritable();
	private MapWritable tags = new MapWritable();
	
	public WayRecord() {}
	
	public void addNodeRef(NodeRef nd) {
		nodeRefs.put(nd.getRef(), nd);
	}

	@SuppressWarnings("rawtypes")
	public Collection getNodeRefs() {
		return nodeRefs.values();
	}
	
	public void addTag(Tag tag) {
		tags.put(tag.getK(), tag);
	}
	
	@SuppressWarnings("rawtypes")
	public Collection getTags() {
		return tags.values();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		nodeRefs.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		nodeRefs.readFields(in);
	}
}
