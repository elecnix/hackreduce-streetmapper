package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class ResolvedWay implements Writable {

	private WayRecord way;
	private ArrayWritable nodes;
	
	public ResolvedWay(WayRecord way, Iterable<ResolvedWayNode> resolvedWayNodes) {
		this.way = way;
		ArrayList<NodeRecord> list = new ArrayList<NodeRecord>();
		for (ResolvedWayNode wayNode : resolvedWayNodes) {
			list.add(wayNode.getNode());
		}
		this.nodes = new ArrayWritable(NodeRecord.class, list.toArray(new NodeRecord[0]));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		way.write(out);
		nodes.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		way.readFields(in);
		nodes.readFields(in);
	}

}
