package org.hackreduce.streetmapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.mappers.XMLInputFormat;
import org.hackreduce.mappers.XMLRecordReader;


/**
 * Extends the basic Hadoop {@link Mapper} to process the Bixi XML data dump by
 * accessing {@link BixiRecord}
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class WayMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<WayRecord, Text, Text, K, V> {

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
	protected WayRecord instantiateModel(Text xmlFilename, Text xml) {
		return new WayRecord(xmlFilename, xml);
	}

}