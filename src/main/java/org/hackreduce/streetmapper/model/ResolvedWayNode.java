package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ResolvedWayNode implements Writable {

	private WayRecord way = new WayRecord();
	private NodeRecord node = new NodeRecord();
	
	public ResolvedWayNode() {
	}
	
	public ResolvedWayNode(WayRecord way, NodeRecord node) {
		this.way = way;
		this.node = node;
	}
	
	public WayRecord getWay() {
		return way;
	}

	public void setWay(WayRecord way) {
		this.way = way;
	}

	public NodeRecord getNode() {
		return node;
	}

	public void setNode(NodeRecord node) {
		this.node = node;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		way.write(out);
		node.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		way.readFields(in);
		node.readFields(in);
	}

}
