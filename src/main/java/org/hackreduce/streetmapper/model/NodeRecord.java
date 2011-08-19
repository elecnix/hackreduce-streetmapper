package org.hackreduce.streetmapper.model;

import org.apache.hadoop.io.Text;

public class NodeRecord extends OsmRecord {

	public NodeRecord() {}
	
	public NodeRecord(Text xmlFilename, Text xml) {
		super(xmlFilename, xml);
	}

}
