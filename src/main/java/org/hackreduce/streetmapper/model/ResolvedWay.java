package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ResolvedWay implements Writable {

	private WayRecord way;
	private ArrayWritable nodes;
	
	public ResolvedWay(Configuration conf, Iterable<ResolvedWayNode> resolvedWayNodes) throws IOException {
		ArrayList<NodeRecord> list = new ArrayList<NodeRecord>();
		for (ResolvedWayNode wayNode : resolvedWayNodes) {
			this.way = wayNode.getWay();
			list.add(ReflectionUtils.copy(conf, wayNode.getNode(), new NodeRecord()));
		}
		this.nodes = new ArrayWritable(NodeRecord.class, list.toArray(new NodeRecord[list.size()]));
	}

	public WayRecord getWay() {
		return way;
	}

	public void setWay(WayRecord way) {
		this.way = way;
	}

	public ArrayWritable getNodes() {
		return nodes;
	}

	public void setNodes(ArrayWritable nodes) {
		this.nodes = nodes;
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
