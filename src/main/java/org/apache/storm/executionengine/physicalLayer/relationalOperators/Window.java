package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.storm.executionengine.topologyLayer.Emitter;

public interface Window extends Serializable {

	void prepare(Map stormConf, ExecutorService executor, String streamId);

	void addEmitter(Emitter emitter);

	void cleanup();

	boolean isWindowOperator();
}
