package master.storm.esper.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;


public class TopologyAgg {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tempSpout", new TemperatureSpout());
		builder.setBolt("filterBolt", new FilterBolt(TemperatureSpout.TEMPERATURE_FIELDNAME, 100, "A1"))
			.shuffleGrouping("tempSpout",TemperatureSpout.TEMPERATURE_STREAMNAME);
		
		builder.setBolt("aggBolt", new AggregateBolt(TemperatureSpout.TEMPERATURE_FIELDNAME, 5),2)
		.fieldsGrouping("tempSpout",TemperatureSpout.TEMPERATURE_STREAMNAME, new Fields(TemperatureSpout.ROOM_FIELDNAME));
		
		builder.setBolt("filterBoltA2", new FilterBolt(AggregateBolt.AVG_TEMPERATURE_FIELDNAME, 70, "A2"))
			.shuffleGrouping("aggBolt",AggregateBolt.AVG_TEMPERATURE_STREAMNAME);
		
//		LocalCluster lcluster = new LocalCluster();
//		lcluster.submitTopology("TopologyAgg", new Config(), builder.createTopology());
//		
//		//LEAVING IT RUN FOR 30 SECONDS...
//		Utils.sleep(100000);
//		
//		lcluster.shutdown();
		
		Config config = new  Config();
		config.setNumWorkers(Integer.parseInt(args[0]));
		StormSubmitter.submitTopology("Test", config, builder.createTopology());
		
	}
}
