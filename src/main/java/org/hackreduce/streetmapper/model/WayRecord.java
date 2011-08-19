package org.hackreduce.streetmapper.model;

import org.apache.hadoop.io.Text;

public class WayRecord extends OsmRecord {

	public WayRecord(Text xmlFilename, Text xml) {
		super(xmlFilename, xml);
	}

}
