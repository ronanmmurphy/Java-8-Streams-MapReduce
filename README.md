# Java-8-Streams-MapReduce
Implemented MapReduce using Java 8 streams for Weather Station Objects.
The purpose of this task is parellel stream method countTemperatures(t1,t2,r) in the class WeatherStation which returns a list which contains two pairs: 
1) temperature t1 paired with the number of times a temperature in the interval [t1-r..t1+r] has been measured so far by any of the weather stations in stations
2) temperature t2 paired with the number of times a temperature in the interval [t2-r..t2+r] has been measured so far by any of the weather stations in stations. 
This method emulates MapReduce accomplised with Clustering frameworks like Hadoop but mapping to (key,value) pairs which are reduced with sum computation at the end of the stream.
The other method maxTemperature returns the highest temperature recorded bewteen the interval. 
