package master.storm.esper.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;


public class Topology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tempSpout", new TemperatureSpout());
		builder.setBolt("filterBolt", new FilterBolt(TemperatureSpout.TEMPERATURE_FIELDNAME, 100, "A1"))
			.shuffleGrouping("tempSpout",TemperatureSpout.TEMPERATURE_STREAMNAME);
		builder.setBolt("filterBoltA2", new FilterBolt(TemperatureSpout.TEMPERATURE_FIELDNAME, 70, "A2"))
			.shuffleGrouping("tempSpout",TemperatureSpout.TEMPERATURE_STREAMNAME);
		
		LocalCluster lcluster = new LocalCluster();
		lcluster.submitTopology("ConverterTopology", new Config(), builder.createTopology());
		
		//LEAVING IT RUN FOR 30 SECONDS...
		Utils.sleep(30000);
		
		lcluster.shutdown();
	}
}
