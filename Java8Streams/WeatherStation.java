//Ronan Murphy: 15397831
//Assignment 2
package Assignment2;
import java.util.*;
import java.util.stream.Collectors;


public class WeatherStation {
    private String city; //city string created
    private List<Measurement> measurements = new ArrayList<Measurement>(); //measurements list created
    private static List<WeatherStation> stations = new ArrayList<WeatherStation>(); //weather stations list created

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

    public static void countTemperatures(double t1, double t2, double r){
        //create arraylist instance of Lister class to output data
        ArrayList<Lister> output = new ArrayList<Lister>();
        //MapReduce Methods for T1 and T2
        //method to return count of T1
        //runs a parallell stream to map the measuremnts in each station filtering T1+-r
        //it maps the temperature value of this to remove the time from measurements
        //collects to a map where the key is donated by the city which is mapped to 1 for each as its not necessary to sort for this example
        //counts the values in the filtered range using 'sum'
        //flatmap used to reduce stream to one dimension of the sum of total values for T1 which are reduced and returned
        int count = stations.parallelStream().map(station -> station.measurements.stream()
                .filter(e -> t1 +r >= e.getTemperature()).filter(e -> t1 -r <= e.getTemperature()).map(v -> v.getTemperature())
                .collect(Collectors.toMap(a -> station.getCity(), a-> 1, Integer::sum)))
                .flatMap(ct ->ct.values().stream()).reduce(Integer::sum).orElse(0);
        Lister list1 = new Lister(t1, count);

        //method to return count of T2
        //acts in exact same way as explained above for the T2 value over the range T2+-r
        int count2 = stations.parallelStream().map(station -> station.measurements.stream()
                .filter(e -> t2 +r >= e.getTemperature()).filter(e -> t2 -r <= e.getTemperature()).map(s -> s.getTemperature())
                .collect(Collectors.toMap(p -> station.getCity(), p-> 1, Integer::sum)))
                .flatMap(ct ->ct.values().stream()).reduce(Integer::sum).orElse(0);
        Lister list2 = new Lister(t2, count2);

        //add the two objects to the arraylist output and print
        output.add(list1);
        output.add(list2);

        System.out.println(output);

    }

    public static void main(String args[]){
        //main method to test code adding sample measurements for each station
        //to an array list for their respective stations
        Measurement m1 = new Measurement(1, 20.0);
        Measurement m2 = new Measurement(13, 11.7);
        Measurement m3 = new Measurement(25, -5.4);
        Measurement m4 = new Measurement(55, 18.7);
        Measurement m5 = new Measurement(77, 20.9);
        List<Measurement> NY = new ArrayList<>();
        NY.add(m1);
        NY.add(m2);
        NY.add(m3);
        NY.add(m4);
        NY.add(m5);
        Measurement m6 = new Measurement(6, 8.4);
        Measurement m7 = new Measurement(22, 19.2);
        Measurement m8 = new Measurement(48, 7.2);
        List<Measurement> Boston = new ArrayList<>();
        Boston.add(m6);
        Boston.add(m7);
        Boston.add(m8);

        WeatherStation station1 = new WeatherStation("New York", NY);
        WeatherStation station2 = new WeatherStation("Boston", Boston);

        stations.add(station1);
        stations.add(station2);

        System.out.println("Part 1");
        station1.maxTemperature(0, 50);// Applying the maxTemperature method
        station2.maxTemperature(0, 50);

        System.out.println("Part 2");
        countTemperatures(19.0,10.8,2.1); // Applying countTemperatures method
    }

}
