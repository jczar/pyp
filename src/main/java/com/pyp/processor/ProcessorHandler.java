package com.pyp.processor;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.pyp.data.PypEventMessage;

public class ProcessorHandler implements Processor, Iterable<Processor> {
	private Set<Processor> processors = new LinkedHashSet<>(); 

	public void setup() {
		for (Processor p: processors) {
			p.setup();
		}
	}
	
	public void processEventMessage(PypEventMessage pypEvent) {
		for (Processor p: processors) {
			p.processEventMessage(pypEvent);
		}		
	}
	
	public void close() {
		for (Processor p: processors) {
			p.close();
		}		
	}
	
	public void addProcessor(Processor processor) {
		processors.add(processor);
	}
	
	public Iterator<Processor> iterator() {
		return processors.iterator();
	}
	
}
