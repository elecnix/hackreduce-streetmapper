package org.hackreduce.streetmapper.stax;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.hackreduce.streetmapper.model.NodeRecord;
import org.hackreduce.streetmapper.model.WayNodeRecord;
import org.hackreduce.streetmapper.model.WayRecord;

public class OsmRecordReader extends StaxRecordReader<WayNodeRecord> {

	@Override
	protected WayNodeRecord parseRecord(XMLStreamReader reader) throws XMLStreamException {
		while (true) {
		    int event = reader.next();
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
		}
	}

}
