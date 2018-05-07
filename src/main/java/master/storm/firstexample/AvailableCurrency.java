package master.storm.firstexample;

public class AvailableCurrency {

	public enum Currency{
		USD(0.78552), GBG(1.26994), JPY(0.00728);
		private double value;
		private Currency(double value){
			this.value=value;
		}
	}
	
	public static Currency getRandomCurrency(){
		int index = (int) (Math.random()*3);
		return Currency.values()[index];
	}
	
	public static double getRate(Currency currency){
		return currency.value;
	}
	
}
