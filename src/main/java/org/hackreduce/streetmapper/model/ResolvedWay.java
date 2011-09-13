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

import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;

public class ResolvedWay implements Writable {

	private WayRecord way = new WayRecord();
	private ArrayWritable nodes = new ArrayWritable(NodeRecord.class);
	
	public ResolvedWay() {
	}
	
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

	public double getLengthInMeters() {
		double length = 0;
		LatLng previousPoint = null;
		NodeRecord previousNode = null;
		for (Object nodeObj : getNodes().get()) {
			NodeRecord node = (NodeRecord) nodeObj;
			LatLng point = new LatLng(node.getLat().get(), node.getLon().get());
			if (previousPoint != null) {
				double distance = LatLngTool.distance(previousPoint, point, LengthUnit.METER);
//				System.out.println("Way " + resolvedWay.getWay().getId() + " segment from " + previousNode.getId() + " " + previousPoint + " to " + node.getId() + " " + point + ": " + distance + " meters");
				length += distance;
			}
			previousNode = node;
			previousPoint = point;
		}
		return length;
	};

}
