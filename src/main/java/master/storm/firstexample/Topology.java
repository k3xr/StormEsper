package master.storm.firstexample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class Topology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("currencySpout", new CurrencySpout());
		builder.setBolt("converterBolt", new ConverterBolt()).shuffleGrouping("currencySpout",CurrencySpout.CURRENCYOUTSTREAM);
		LocalCluster lcluster = new LocalCluster();
		lcluster.submitTopology("ConverterTopology", new Config(), builder.createTopology());
		
		Utils.sleep(10000);
		
		lcluster.shutdown();
	}
}
