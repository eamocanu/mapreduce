package com.sandbox

import net.liftweb.json._
import MapReduceJob._

/**
  * Created by amocanu on 1/27/2018.
  */
object MapReduceJobs {

  class WordCount extends Mapper with Reducer {
    val w = """[\W']+"""
    //different than in python bc it splits `don't` into 2 tokens

    def map(line: String): Iterable[(String, Any)] = {
      //possible optimization: can do partial reduce here before sending over network
      line.split(w).map(word => word -> 1.0).toList
    }

    def reduce(key: String, values: Iterable[(String, Any)]): (String, Any) = {
      key -> values.unzip._2.map(_.toString.toDouble).reduce(_ + _)
    }
  }

  class MoviesMapper extends Mapper {
    implicit val formats = DefaultFormats

    case class Movie(id: String, name: String)

    def map(line: String): Iterable[(String, Any)] = {
      val movie = parse(line.trim).extract[Movie]
      List(movie.id -> movie.name)
    }
  }

  class RatingsMapper extends Mapper {
    implicit val formats = DefaultFormats

    case class Rating(user_id: String, movie_id: String, rating: String)

    def map(line: String): Iterable[(String, Any)] = {
      val rating = parse(line.trim).extract[Rating]
      List(rating.movie_id -> rating.rating)
    }
  }

  class AvgMovieRatingReducer extends Reducer {
    //movieID                 (file, rating) | (file, movie)
    def reduce(key: String, values: Iterable[(String, Any)]): (String, Any) = {
      val mapp = values.groupBy { case (key, values) => key }
        .map { case (x, xs) => x -> xs.unzip._2.toList }

      val movieTitle = mapp.get("movies.txt").map(_.map(_.toString).head)
      val ratings = mapp.get("ratings.txt").map(_.map(_.toString.toDouble))
      val avgRating = ratings.map(r => r.reduce(_ + _) / r.size)

      movieTitle.flatMap(t => avgRating.map(a => t -> a)).get
    }
  }

}