package com.sandbox

import MapReduceJobs._
import MapReduceJob._

/**
  * Created by amocanu on 1/27/2018.
  */

object Main {
  def main(args: Array[String]): Unit = {
    val k = 1
    val parallelism = Runtime.getRuntime.availableProcessors * k
    val multiThreadedSystem = new MultiThreadedMapReduceSystem(parallelism)
    val singleThreadedSystem = new SingleThreadedMapReduceSystem

    println("singleThreadedSystem")
    runMapReduceJobs(singleThreadedSystem)
    println("\nmultiThreadedSystem")
    runMapReduceJobs(multiThreadedSystem)

    multiThreadedSystem.shutdown
    singleThreadedSystem.shutdown
  }


  def runMapReduceJobs(mrSystem:MapReduceSystem) = {
    val inputFile = FileUtils.shopifyWords.split("\r\n").toList
    val wc = new WordCount
    val job = new MapReduceJob(List(MapperPayload(wc, "story.txt", inputFile)), wc)
    mrSystem.run(job)
    val wordCounts = mrSystem.collect
    wordCounts.foreach(println(_))

    println

    val moviesFile = FileUtils.movies.split("\r\n").toList
    val ratingsFile = FileUtils.ratings.split("\r\n").toList
    val job2 = new MapReduceJob(List(MapperPayload(new MoviesMapper, "movies.txt", moviesFile),
      MapperPayload(new RatingsMapper, "ratings.txt", ratingsFile)), new AvgMovieRatingReducer)
    mrSystem.run(job2)
    val movieRatings = mrSystem.collect
    movieRatings.foreach(println(_))
  }

}