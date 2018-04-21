package master.storm.esper.example;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class FilterBolt extends BaseRichBolt{

	private String fieldname;
	private int thr;
	private String alarmLabel;

	public FilterBolt (String fieldname, int thr, String alarmLabel){
		this.fieldname = fieldname;
		this.thr = thr;
		this.alarmLabel = alarmLabel;
	}
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		//NOTHING TO PREPARE 
		
	}

	public void execute(Tuple input) {
//		System.out.println("Received: " + input.getValues());
		double valueByField = (Double) input.getValueByField(fieldname);
		
		if(valueByField > thr){
			System.out.println(alarmLabel + ", " + input.getValueByField(TemperatureSpout.ROOM_FIELDNAME) +
					", " + valueByField);
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// NO OUTPUTS
		
	}

}
