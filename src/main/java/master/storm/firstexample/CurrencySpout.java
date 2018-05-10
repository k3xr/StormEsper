package master.storm.firstexample;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class CurrencySpout extends BaseRichSpout{

	public static final String CURRENCYOUTSTREAM = "currencyStream";
	public static final String CURRENCYFIELDNAME = "currencyID";
	public static final String CURRENCYFIELDVALUE = "value";
	private SpoutOutputCollector collecotor;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collecotor=collector;
	}

	public void nextTuple() {
		Values randomValue = this.randomValue();
		System.out.println("emitting " + randomValue);
		collecotor.emit(CURRENCYOUTSTREAM, randomValue);
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(CURRENCYOUTSTREAM, new Fields(CURRENCYFIELDNAME, CURRENCYFIELDVALUE));
	}
	
	private Values randomValue(){
		double value = Math.random()*1000;
		return new Values(AvailableCurrency.getRandomCurrency(), value);
	}

}
