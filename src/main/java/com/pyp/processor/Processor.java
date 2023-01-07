package com.pyp.processor;

import com.pyp.data.PypEventMessage;

public interface Processor  {
	
	// Should be implemented as a Chain of responsibility
	
	public void setup();
	
	public void processEventMessage(PypEventMessage eventMessage);
	
	public void close();
	
}
