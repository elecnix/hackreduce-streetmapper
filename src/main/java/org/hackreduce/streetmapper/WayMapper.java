package org.hackreduce.streetmapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.mappers.XMLInputFormat;
import org.hackreduce.mappers.XMLRecordReader;
import org.hackreduce.streetmapper.model.OsmRecord;

/**
 * Extends the basic Hadoop {@link Mapper} to process the OpenStreetMap XML data by
 * accessing {@link OsmRecord}.
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class WayMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<OsmRecord, Text, Text, K, V> {

	public static void configureJob(Job job) {
		job.setInputFormatClass(XMLInputFormat.class);
		XMLRecordReader.setRecordTags(job, "<way>", "</way>");
	}

	@Override
	protected OsmRecord instantiateModel(Text xmlFilename, Text xml) {
		return new OsmRecordParser().parse(xml);
	}

}