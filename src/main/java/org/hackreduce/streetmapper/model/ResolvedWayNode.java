package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ResolvedWayNode implements WritableComparable<ResolvedWayNode> {

	private WayRecord way = new WayRecord();
	private NodeRecord node = new NodeRecord();
	private IntWritable nodeIndex = new IntWritable(-1);
	
	public ResolvedWayNode() {
	}
	
	public ResolvedWayNode(WayRecord way, NodeRecord node, int nodeIndex) {
		this.way = way;
		this.node = node;
		this.setNodeIndex(new IntWritable(nodeIndex));
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
	
	public IntWritable getNodeIndex() {
		return nodeIndex;
	}
	
	public void setNodeIndex(IntWritable nodeIndex) {
		this.nodeIndex = nodeIndex;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		assert nodeIndex.get() != -1;
		way.write(out);
		node.write(out);
		nodeIndex.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		way.readFields(in);
		node.readFields(in);
		nodeIndex.readFields(in);
		assert nodeIndex.get() != -1;
	}

	@Override
	public int compareTo(ResolvedWayNode o) {
		return this.nodeIndex.compareTo(o.nodeIndex);
	}

}
