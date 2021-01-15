//Ronan Murphy: 15397831
//Assignment 2
package Assignment2;

public class Lister {
    private double temperature;
    private int counter;
    //class to return the list of temperatures and count of the temperature in range t1, t2 +-r
    public Lister(double temperature, int counter){
        this.counter = counter;
        this.temperature = temperature;
    }
    //returns to string value so its viewable on print output
    public String toString(){ return "(" + temperature + ", " + counter + ")"; }
}
