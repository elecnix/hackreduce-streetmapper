package org.hackreduce.streetmapper.stax;

import java.util.logging.Logger;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.hackreduce.mappers.XMLRecordReader;
import org.hackreduce.streetmapper.model.NodeRecord;
import org.hackreduce.streetmapper.model.WayNodeRecord;
import org.hackreduce.streetmapper.model.WayRecord;

public class OsmRecordReader extends StaxRecordReader<WayNodeRecord> {

	private static final Logger LOG = Logger.getLogger(XMLRecordReader.class.getName());
	
	@Override
	protected WayNodeRecord parseRecord(XMLStreamReader reader) {
		while (true) {
			try {
				int event;
				event = reader.next();
				if (event == XMLStreamConstants.END_DOCUMENT) {
					reader.close();
					return null;
				}
				if (event == XMLStreamConstants.START_ELEMENT) {
					if (reader.getLocalName().equals("way")) {
						return new WayNodeRecord(new WayRecord());
					} else if (reader.getLocalName().equals("node")) {
						return new WayNodeRecord(new NodeRecord());
					}
				}
			} catch (XMLStreamException e) {
				LOG.info("Ignored glitch: " + e.getMessage());
			}
		}
	}

}
