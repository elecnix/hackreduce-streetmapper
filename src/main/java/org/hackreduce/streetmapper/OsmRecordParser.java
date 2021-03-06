package org.hackreduce.streetmapper;

import java.io.ByteArrayInputStream;
import java.util.Iterator;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.hackreduce.streetmapper.model.NodeId;
import org.hackreduce.streetmapper.model.NodeRecord;
import org.hackreduce.streetmapper.model.OsmRecord;
import org.hackreduce.streetmapper.model.Tag;
import org.hackreduce.streetmapper.model.WayId;
import org.hackreduce.streetmapper.model.WayRecord;

public class OsmRecordParser {

	private static XMLInputFactory factory;

	private static Logger LOG = Logger.getLogger(OsmRecordParser.class.getName());

	public OsmRecordParser() {
		factory = XMLInputFactory.newInstance();
	}

	public OsmRecord parse(Text xml) {
		try {
			return innerParse(xml);
		} catch (XMLStreamException e) {
			LOG.error("Error parsing:\n" + xml, e);
			throw new RuntimeException(e);
		}
	}

	private OsmRecord innerParse(Text xml) throws XMLStreamException {
		XMLEventReader reader = factory.createXMLEventReader(new ByteArrayInputStream(xml.getBytes()));
		try {
			XMLEvent event = reader.nextTag();
			StartElement startElement = event.asStartElement();
			String elemName = startElement.getName().getLocalPart();
			if ("way".equals(elemName)) {
				return parseWay(reader, startElement);
			} else if ("node".equals(elemName)) {
				return parseNode(reader, startElement);
			}
			throw new RuntimeException("Unknown element: " + elemName);
		} finally {
			reader.close();
		}
	}

	private NodeRecord parseNode(XMLEventReader reader, StartElement startElement) {
		NodeRecord node = new NodeRecord();
		@SuppressWarnings("unchecked")
		Iterator<Attribute> attrs = startElement.getAttributes();
		while (attrs.hasNext()) {
			Attribute att = attrs.next();
			readNodeAttribute(node, att);
		}
		return node;
	}

	private void readNodeAttribute(NodeRecord node, Attribute att) {
		//   <node id='20574043' timestamp='2008-08-04T03:40:49Z' uid='1626' user='FredB' visible='true' version='3' changeset='54783' lat='46.7891071' lon='-71.2258505'>
		String localName = att.getName().getLocalPart();
		if ("id".equals(localName)) {
			node.setId(new NodeId(Long.parseLong(att.getValue())));
		} else if ("lat".equals(localName)) {
			node.setLat(new FloatWritable(Float.parseFloat(att.getValue())));
		} else if ("lon".equals(localName)) {
			node.setLon(new FloatWritable(Float.parseFloat(att.getValue())));
		} else {
			readCommonAttribute(node, att);
		}
	}

	private void readCommonAttribute(OsmRecord record, Attribute att) {
		String localName = att.getName().getLocalPart();
		if ("uid".equals(localName)) {
			record.setUid(new LongWritable(Long.parseLong(att.getValue())));
		} else if ("changeset".equals(localName)) {
			record.setChangeset(new LongWritable(Long.parseLong(att.getValue())));
		} else if ("version".equals(localName)) {
			record.setVersion(new IntWritable(Integer.parseInt(att.getValue())));
		} else if ("timestamp".equals(localName)) {
			record.setTimestamp(new Text(att.getValue()));
		} else if ("user".equals(localName)) {
			record.setUser(new Text(att.getValue()));
		} else if ("visible".equals(localName)) {
			record.setVisible(new BooleanWritable("true".equalsIgnoreCase(att.getValue())));
		} else {
			throw new RuntimeException("Unknown attribute: " + localName + " for " + OsmRecord.class.getSimpleName());
		}
	}

	private WayRecord parseWay(XMLEventReader reader, StartElement startElement) throws XMLStreamException {
		//   <way id='27073111' timestamp='2009-08-13T01:33:38Z' uid='37993' user='fsteggink' visible='true' version='7' changeset='2125488'>
		//     <nd ref='1219686016' />
		//     <tag k='castle_type' v='citadel' />
		WayRecord way = new WayRecord();
		@SuppressWarnings("unchecked")
		Iterator<Attribute> attrs = startElement.getAttributes();
		while (attrs.hasNext()) {
			Attribute att = attrs.next();
			String localName = att.getName().getLocalPart();
			if ("id".equals(localName)) {
				way.setId(new WayId(Long.parseLong(att.getValue())));
			} else {
				readCommonAttribute(way, att);
			}
		}
		while (reader.hasNext()) {
			XMLEvent xmlEvent = reader.nextEvent();
			if (xmlEvent.isStartElement()) {
				StartElement startElement2 = xmlEvent.asStartElement();
				String elemName = startElement2.getName().getLocalPart();
				if ("nd".equals(elemName)) {
					way.addNodeRef(parseNd(reader, startElement2));
				} else if ("tag".equals(elemName)) {
					way.addTag(parseTag(reader, startElement2));
				} else {
					throw new RuntimeException("Unknown element: " + elemName + " in " + startElement.getName());
				}
			}
		}
		return way;
	}

	private NodeId parseNd(XMLEventReader reader, StartElement startElement) {
		NodeId nodeRef = new NodeId();
		@SuppressWarnings("unchecked")
		Iterator<Attribute> attrs = startElement.getAttributes();
		while (attrs.hasNext()) {
			Attribute att = attrs.next();
			readNodeRefAttribute(nodeRef, att);
		}
		return nodeRef;
	}

	private void readNodeRefAttribute(NodeId nodeRef, Attribute att) {
		String localName = att.getName().getLocalPart();
		if ("ref".equals(localName)) {
			nodeRef.set(Long.parseLong(att.getValue()));
		} else {
			throw new RuntimeException("Unknown attribute: " + localName + " for " + NodeId.class.getSimpleName());
		}
	}

	private Tag parseTag(XMLEventReader reader, StartElement startElement) {
		Tag tag = new Tag();
		@SuppressWarnings("unchecked")
		Iterator<Attribute> attrs = startElement.getAttributes();
		while (attrs.hasNext()) {
			Attribute att = attrs.next();
			readTagAttribute(tag, att);
		}
		return tag;
	}

	private void readTagAttribute(Tag tag, Attribute att) {
		String localName = att.getName().getLocalPart();
		if ("k".equals(localName)) {
			tag.setK(new Text(att.getValue()));
		} else if ("v".equals(localName)) {
			tag.setV(new Text(att.getValue()));
		} else {
			throw new RuntimeException("Unknown attribute: " + localName + " for " + Tag.class.getSimpleName() + ": " + tag);
		}
	}

}
