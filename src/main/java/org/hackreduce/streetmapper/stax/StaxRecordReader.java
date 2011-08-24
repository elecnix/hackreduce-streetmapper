package org.hackreduce.streetmapper.stax;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import javax.xml.stream.Location;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLReporter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.hackreduce.mappers.XMLRecordReader;

import com.sun.org.apache.xerces.internal.impl.XMLDocumentScannerImpl;
import com.sun.org.apache.xerces.internal.impl.XMLErrorReporter;
import com.sun.org.apache.xerces.internal.impl.XMLStreamReaderImpl;
import com.sun.org.apache.xerces.internal.xni.XNIException;

public abstract class StaxRecordReader<R> extends RecordReader<Text, R> {

	private static final Logger LOG = Logger.getLogger(XMLRecordReader.class.getName());

	private FileSplit _split;
	private long _start;
	private long _end;
	private long _pos;
	private Text _fileName;
	private CompressionCodecFactory _compressionCodecs;
	private SoftBoundedRangeFileInputStream _inputStream;
	private XMLStreamReader _reader;
	private Text _key;
	private R _value;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        _split = (FileSplit)split;
        _start = _split.getStart();
        _end = _start + _split.getLength();
        
        _pos = _start;
        
        final Path file = _split.getPath();
        _fileName = new Text(file.getName());
        _compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = _compressionCodecs.getCodec(file);
        
        LOG.info("XMLRecordReader.initialize: " + " start=" + _start + " end=" + _end);
        
        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(conf);
        _inputStream = new SoftBoundedRangeFileInputStream(fs.open(file), _start, _split.getLength());
        InputStream in = _inputStream;
        if (codec != null) {
        	in = new BufferedInputStream(codec.createInputStream(_inputStream));
        }
		final XMLInputFactory factory = XMLInputFactory.newInstance();
		factory.setXMLReporter(new XMLReporter() {
			@Override
			public void report(String message, String errorType,
					Object relatedInformation, Location location)
					throws XMLStreamException {
				LOG.info("blah");
			}
		});
		try {
			_reader = factory.createXMLStreamReader(in);

			// Because a split can occur anywhere in the XML document, the parser will complain.
			// We silence it off, since we know the previous split will read until it reached a
			// full record, even if that means reading past the end of the split.
//			((XMLStreamReaderImpl) _reader).getScanner().setProperty("http://apache.org/xml/properties/internal/error-reporter", new XMLErrorReporter() {
//				@Override
//				public void reportError(String domain, String key,
//						Object[] arguments, short severity) throws XNIException {
//					try {
//						super.reportError(domain, key, arguments, severity);
//					} catch (XNIException e) {
//						LOG.info("Ignoring: " + e);
//					}
//				}
//			});
		} catch (XMLStreamException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO check number of bytes read from input stream to finish at end of split
		if (_pos >= _end) {
			return false;
		}
		_key = _fileName;
		_value = parseRecord(_reader);
		return _value != null;
	}

	protected abstract R parseRecord(XMLStreamReader reader);

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
        return _key;
	}

	@Override
	public R getCurrentValue() throws IOException, InterruptedException {
		return _value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
        if (_start == _end) {
            return 0.0f;
          } else {
            return Math.min(1.0f, (_pos - _start) / (float)(_end - _start));
          }
	}

	@Override
	public void close() throws IOException {
		try {
			_reader.close();
		} catch (XMLStreamException e) {
			throw new IOException(e);
		}
	}
}
