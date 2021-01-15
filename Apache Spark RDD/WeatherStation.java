//Ronan Murphy: 15397831
//Assignment 3 : Part 1
package assignment3a;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//implement serializable for 
public class WeatherStation implements Serializable{
	
    private String city; //city string created
    private List<Measurement> measurements = new ArrayList<Measurement>(); //measurements list created
    private static List<WeatherStation> stations = new ArrayList<WeatherStation>(); //weather stations list created
    static List<Measurement> NY = new ArrayList<Measurement>();
    static List<Measurement> Boston = new ArrayList<Measurement>();
    //constructor for weather station with city and measurement parameters
    public WeatherStation(String city, List<Measurement> measurements){
        this.city = city;
        this.measurements = measurements;
    }
    //setters and getters for objects city and measurements
    public void setCity(String city){
        this.city = city;
    }

    public String getCity(){
        return city;
    }

    public void setMeasurements(List<Measurement> measurements){
        this.measurements = measurements;
    }

    public List<Measurement> getMeasurements(){
        return measurements;
    }
    //max temperature method parameters start and end time
    public void maxTemperature(int startTime, int endTime){
        //max temp calculated - measurements in list streamed with filter of start and end time and
        // returns double stream of the max value streamed or else returns 0 if no max
        double temps = this.getMeasurements().stream().filter(e -> e.getTime()>=startTime && e.getTime()<=endTime)
                .mapToDouble(e -> e.getTemperature()).max().orElse(0);
        //when called prints max temperature of each station in stream below
        System.out.println("The maximum temperature for " + getCity() +" was: "+ temps);
    }

   //new method to count temperature with double temp as parameter
    public static void countTemperature(double temp) {
    	//set property to hadoop directory 
    	//initalize spark configuration and spark context 
    	System.setProperty("hadoop.home.dir", "C:/winutils");
		SparkConf sparkConf = new SparkConf().setAppName("Weather Count")
				.setMaster("local[2]");
		JavaSparkContext sparkcontext = new JavaSparkContext(sparkConf);
		
		
		//create java rdd object to parallel stream through temperature measuremnts in range +/- 1 and add to a list MR
		JavaRDD<Object> MR = sparkcontext.parallelize(stations)
				.map(stations -> stations.measurements.stream()
				.filter(e -> temp+1 >= e.getTemperature() && temp-1 <= e.getTemperature())
				.map(s -> s.getTemperature())
				.collect(Collectors.toList()));
			
		
		// count the amount of objects in MR list and return this value as the temperatures in the range
		long count = MR.count();
		System.out.println("The count of temperatures in the range " + temp +" +/-1 is: " + count);
		
		
		//close and stop the spark context
		sparkcontext.stop();
		sparkcontext.close();
    }
    
    public static void main(String args[])throws Exception {
		
    	//main method to test code adding sample measurements for each station
        //to an array list for their respective stations
        Measurement m1 = new Measurement(1, 20.0);
        Measurement m2 = new Measurement(13, 11.7);
        Measurement m3 = new Measurement(25, -5.4);
        Measurement m4 = new Measurement(55, 18.7);
        Measurement m5 = new Measurement(77, 20.9);
        NY.add(m1);
        NY.add(m2);
        NY.add(m3);
        NY.add(m4);
        NY.add(m5);
        Measurement m6 = new Measurement(6, 8.4);
        Measurement m7 = new Measurement(22, 19.2);
        Measurement m8 = new Measurement(48, 7.2);
        Boston.add(m6);
        Boston.add(m7);
        Boston.add(m8);
        WeatherStation station1 = new WeatherStation("New York", NY);
        WeatherStation station2 = new WeatherStation("Boston", Boston);
        stations.add(station1);
        stations.add(station2);

        countTemperature(19.0); // Applying countTemperatures method
    }

}
