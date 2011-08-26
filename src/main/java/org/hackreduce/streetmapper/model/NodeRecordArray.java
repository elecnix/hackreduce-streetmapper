package org.hackreduce.streetmapper.model;

import org.apache.hadoop.io.ArrayWritable;

public class NodeRecordArray extends ArrayWritable {
	public NodeRecordArray() {
		super(NodeRecord.class);
	}
}

