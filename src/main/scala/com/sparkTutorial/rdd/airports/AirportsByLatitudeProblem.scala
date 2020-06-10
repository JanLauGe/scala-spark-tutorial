package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

/* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
   Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

   Each row of the input file contains the following columns:
   Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
   ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

   Sample output:
   "St Anthony", 51.391944
   "Tofino", 49.082222
   ...
 */

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("latitude").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile(path="in/airports.text")
    val airports_north = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)

    val out = airports_north.map(line => {
      val items = line.split(Utils.COMMA_DELIMITER)
      items(1) + ", " + items(6)
    })
    out.saveAsTextFile(path = "out/airports_by_latitude.text")
  }
}
