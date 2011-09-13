package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;

public class WayRecord extends OsmRecord {
	
	private WayId id = new WayId(0);
	private SortedMapWritable nodeRefs = new SortedMapWritable();
	private MapWritable tags = new MapWritable();
	
	public WayRecord() {}

	public WayId getId() {
		return id;
	}

	public void setId(WayId id) {
		this.id = id;
	}
	
	public void addNodeRef(NodeId nd) {
		nodeRefs.put(new IntWritable(nodeRefs.size() - 1), nd);
	}

	@SuppressWarnings("rawtypes")
	public Collection getNodeRefs() {
		return nodeRefs.values();
	}

	public int getNodeIndex(NodeId nodeId) {
		int index = 0;
		for (Writable ref : nodeRefs.values()) {
			if (nodeId.equals(ref)) {
				return index;
			}
			index++;
		}
		return -1;
	}
	
	public void addTag(Tag tag) {
		tags.put(tag.getK(), tag.getV());
	}
	
	public MapWritable getTags() {
		return tags;
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
