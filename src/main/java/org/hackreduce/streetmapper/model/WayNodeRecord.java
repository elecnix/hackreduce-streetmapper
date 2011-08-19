package org.hackreduce.streetmapper.model;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class WayNodeRecord extends GenericWritable {

	private static final Class[] TYPES = {
		WayRecord.class,
		NodeRecord.class
	};
	
	@Override
	protected Class<? extends Writable>[] getTypes() {
		return TYPES;
	}
	
	public WayNodeRecord() {
	}

	public WayNodeRecord(OsmRecord record) {
		set(record);
	}

}
