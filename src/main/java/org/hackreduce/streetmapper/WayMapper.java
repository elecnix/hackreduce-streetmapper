package org.hackreduce.streetmapper;

import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.mappers.XMLInputFormat;
import org.hackreduce.mappers.XMLRecordReader;
import org.hackreduce.streetmapper.model.NodeRecord;
import org.hackreduce.streetmapper.model.OsmRecord;
import org.hackreduce.streetmapper.model.WayRecord;


/**
 * Extends the basic Hadoop {@link Mapper} to process the Bixi XML data dump by
 * accessing {@link BixiRecord}
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class WayMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<OsmRecord, Text, Text, K, V> {

	private static final byte[] WAY_BYTES = "<way".getBytes();
	private static final byte[] NODE_BYTES = "<node".getBytes();

	/**
	 * Configures the MapReduce job to read data from the Bixi data dump.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(XMLInputFormat.class);
		XMLRecordReader.setRecordTags(job, "<way>", "</way>");
	}

	@Override
	protected OsmRecord instantiateModel(Text xmlFilename, Text xml) {
		if (bytesStartsWith(WAY_BYTES, xml.getBytes())) {
			return new WayRecord(xmlFilename, xml);
		} else if (bytesStartsWith(NODE_BYTES, xml.getBytes())) {
			return new NodeRecord(xmlFilename, xml);
		}
		return null;
	}

	private boolean bytesStartsWith(byte[] start, byte[] document) {
		return Arrays.equals(subset(document, 0, start.length), start);
	}

	private byte[] subset(byte[] source, int offset, int length) {
		if (source.length < length) return new byte[0];
		byte[] range = new byte[length];
		System.arraycopy(source, offset, range, 0, length);
		return range;
	}

}