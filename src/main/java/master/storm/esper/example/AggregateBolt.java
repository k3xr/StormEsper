package master.storm.esper.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AggregateBolt extends BaseRichBolt{

	public static final String AVG_TEMPERATURE_STREAMNAME = "avg_tempstream";
	public static final String AVG_TEMPERATURE_FIELDNAME = "avg_temperature";
	public static final String ROOM_FIELDNAME = "roomID";
	private String fieldname;
	private int winSize;
	private HashMap<String, double[]> windows;
	private HashMap<String, Integer> pointers;
	private OutputCollector collector;
	
	public AggregateBolt(String fieldname, int winSize) {
		this.fieldname = fieldname;
		this.winSize = winSize;
		
	}
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		windows = new HashMap<String, double[]>();
		pointers = new HashMap<String, Integer>();
	}

	public void execute(Tuple input) {
		Double temperature = input.getDoubleByField(fieldname);
		String roomId = input.getStringByField(TemperatureSpout.ROOM_FIELDNAME);
		double[] field_w = windows.get(roomId);
		if(field_w == null){
			field_w = new double[winSize];
			field_w[0] = temperature;
			pointers.put(roomId, 0);
			windows.put(roomId, field_w);
		}else{
			int pointer = pointers.get(roomId);
			if(pointer == (winSize-2)){
				field_w[pointer+1] = temperature;
				double totTemperature = 0.0;
				
				for (int i = 0; i < field_w.length; i++) {
					totTemperature += field_w[i];
				}
				pointers.put(roomId, -1);
				Values values = new Values(roomId,totTemperature/(double)winSize);
				System.out.println("AGG: " + values);
				collector.emit(AVG_TEMPERATURE_STREAMNAME, 
						values);
				
			}else{
				field_w[pointer+1] = temperature;
				pointers.put(roomId,(pointer+1));
				windows.put(roomId, field_w);
			}
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(AVG_TEMPERATURE_STREAMNAME, new Fields(ROOM_FIELDNAME, AVG_TEMPERATURE_FIELDNAME));
	}

}
