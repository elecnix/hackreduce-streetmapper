package org.hackreduce.mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.hackreduce.streetmapper.model.OsmRecord;
import org.hackreduce.streetmapper.model.WayNodeRecord;
import org.hackreduce.streetmapper.stax.OsmRecordReader;

public class XMLInputFormat extends FileInputFormat<Text, WayNodeRecord> {

    @Override
    public RecordReader<Text, WayNodeRecord> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new OsmRecordReader();
    }
    
}
