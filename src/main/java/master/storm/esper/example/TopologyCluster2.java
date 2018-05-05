package master.storm.esper.example;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyCluster2 {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tempSpout", new TemperatureSpout());
		builder.setBolt("filterBolt", new FilterBolt(TemperatureSpout.TEMPERATURE_FIELDNAME, 100, "A1"))
			.shuffleGrouping("tempSpout",TemperatureSpout.TEMPERATURE_STREAMNAME);
		builder.setBolt("filterBoltA2", new FilterBolt(TemperatureSpout.TEMPERATURE_FIELDNAME, 70, "A2"))
			.shuffleGrouping("tempSpout",TemperatureSpout.TEMPERATURE_STREAMNAME);
		
		

		Config config = new Config();
		config.setNumWorkers(Integer.parseInt(args[0]));
		StormSubmitter.submitTopology("TemperatureTopology2", config, builder.createTopology());
		
		
		
	}
}
