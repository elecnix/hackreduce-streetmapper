package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.MapWritable;

public class WayRecord extends OsmRecord {
	
	private WayId id = new WayId(0);
	private MapWritable nodeRefs = new MapWritable();
	private MapWritable tags = new MapWritable();
	
	public WayRecord() {}

	public WayId getId() {
		return id;
	}

	public void setId(WayId id) {
		this.id = id;
	}
	
	public void addNodeRef(NodeId nd) {
		nodeRefs.put(nd, nd);
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
		id.write(out);
		nodeRefs.write(out);
		tags.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		id.readFields(in);
		nodeRefs.readFields(in);
		tags.readFields(in);
	}
}
