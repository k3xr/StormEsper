package master.storm.firstexample;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import master.storm.firstexample.AvailableCurrency.Currency;

public class ConverterBolt extends BaseRichBolt{

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		//nothing to prepare
	}

	public void execute(Tuple input) {
		Currency currencyID = (Currency) input.getValueByField(CurrencySpout.CURRENCYFIELDNAME);
		double valueField = input.getDoubleByField(CurrencySpout.CURRENCYFIELDVALUE); 
		double rate = AvailableCurrency.getRate(currencyID);
		double euroValue = valueField*rate;
		System.out.println("EUR: " + euroValue + ", " + currencyID + ": " + valueField);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//no output stream
	}

}
