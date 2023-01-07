package com.pyp.processor;

import java.util.function.Supplier;

public enum ProcessorFactory {
	
	BROKER_STATE(BrokerProcessor::new),
	NODE_STATE(NodeProcessor::new);
	
	private Supplier<Processor> instantiator;
	
	public Processor getInstance() {
		return instantiator.get();
	}

	ProcessorFactory(Supplier<Processor> instantiator) {
		this.instantiator = instantiator;
	}
}