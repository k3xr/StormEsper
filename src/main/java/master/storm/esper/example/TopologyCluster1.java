package master.storm.esper.example;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyCluster1 {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tempSpout", new TemperatureSpout());
		FilterBolt filterBolt1 = new FilterBolt(TemperatureSpout.TEMPERATURE_FIELDNAME, 100, "A1");
		builder.setBolt("filterBolt", filterBolt1, 4).setNumTasks(8)
			.shuffleGrouping("tempSpout",TemperatureSpout.TEMPERATURE_STREAMNAME);
		builder.setBolt("filterBoltA2", new FilterBolt(TemperatureSpout.TEMPERATURE_FIELDNAME, 70, "A2"))
			.fieldsGrouping("tempSpout", TemperatureSpout.TEMPERATURE_STREAMNAME, new Fields("roomId"));
		
		Config config = new Config();
		
		config.setNumWorkers(4);
		config.setDebug(true);
		StormSubmitter.submitTopology("TemperatureTopology1", config, builder.createTopology());
		
		
		
	}
}
