package org.apache.storm;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.PigServer;
import org.apache.storm.executionengine.topologyLayer.StormLauncher;
import org.apache.storm.executionengine.topologyLayer.plans.TopologyOperatorPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.yaml.snakeyaml.Yaml;

public class WebPOS {

	private static final String DEFAULT_CONF = "storm.zookeeper.servers:\n    - \"localhost\"           \n\nnimbus.host: \"localhost\"\n\ntopology.name: \"test\"\ntopology.workers: 2\n\n";

	private static Map<String, String> optMap = new HashMap<String, String>();

	private StormLauncher stormLauncher = null;

	public void start(String script, String config) throws Exception {
		Yaml yaml = new Yaml();
		if (config == null || config.trim().length() == 0) {
			config = DEFAULT_CONF;
		}
		Map jobConfig = (Map) yaml.load(config);

		PigServer pigServer = new PigServer("local");
		PigContext pc = pigServer.getPigContext();

		LogicalPlan logicalPlan = pigServer.registerStormQuery(script);
		LogicalPlan lp = pigServer.optimizeLogicalPlan(logicalPlan, true);

		org.apache.storm.executionengine.physicalLayer.plans.PhysicalPlan pp = pigServer
				.buildPhysicalPlan(lp);

		TopologyOperatorPlan top = pigServer.buildTopologyPlan(pp);

		System.out.println(top);

		this.stormLauncher = new StormLauncher();
		stormLauncher.lauchWebLocal(top, pc, jobConfig);
	}

	public void stop() {
		this.stormLauncher.stopWebLocal();
	}

	public static void main(String[] args) {
		StringBuffer script = new StringBuffer();
		DataInputStream in = new DataInputStream(new BufferedInputStream(System.in));
		String s;
		System.out.println("YO");
		try {
			while ((s = in.readLine()).length() != 0){
				System.out.println(s);
				script.append(s).append("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(script.length() ==0){
			System.out.println("A script with length 0 is invalid!");
			return;
		}
		WebPOS pos = new WebPOS();
		try {
			pos.start(script.toString(), null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
