package org.hackreduce.streetmapper.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ResolvedWay implements Writable {

	private WayRecord way;
	private ArrayWritable nodes;
	
	public ResolvedWay(Configuration conf, Iterable<ResolvedWayNode> resolvedWayNodes) throws IOException {
		TreeMap<IntWritable, NodeRecord> nodes = new TreeMap<IntWritable, NodeRecord>();
		for (ResolvedWayNode wayNode : resolvedWayNodes) {
			this.way = wayNode.getWay();
			nodes.put(new IntWritable(wayNode.getNodeIndex().get()), ReflectionUtils.copy(conf, wayNode.getNode(), new NodeRecord()));
		}
		this.nodes = new ArrayWritable(NodeRecord.class, nodes.values().toArray(new NodeRecord[nodes.size()]));
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
