/**
  * Created by amocanu on 1/27/2018.
  */


import com.sandbox.MapReduceJob.{MapperPayload, MapReduceJob}
import com.sandbox.MapReduceJobs.{WordCount, AvgMovieRatingReducer, RatingsMapper, MoviesMapper}
import com.sandbox.SingleThreadedMapReduceSystem
import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Properties}
import org.scalatest.MustMatchers
import org.scalacheck._
import Gen._
import Arbitrary.arbitrary

import org.scalacheck.Gen.{choose, containerOf,
nonEmptyContainerOf, containerOfN, oneOf}


object MRSpec extends Properties("SingleThreadedMapReduceSystem") with MustMatchers {
  val singleThreadedSystem = new SingleThreadedMapReduceSystem

  val genStringStream = Gen.nonEmptyContainerOf[List, String](Gen.oneOf("your", "parrot", "talks", "a", "lot", "bamboo", "fire", "wind", "water", "sky", "topic", "explosive", "car"))

  def testWordCount = {
    val wc = new WordCount

    implicit lazy val arbPayload: Arbitrary[MapperPayload] = Arbitrary(
      for {
        fileName <- Gen.identifier
        inputFile <- genStringStream
      } yield MapperPayload(wc, fileName, inputFile)
    )

    implicit lazy val arbJob = Arbitrary(for {
      v <- arbitrary[List[MapperPayload]]
    } yield MapReduceJob(v, wc))

    property("run with WordCount") = forAll((j: MapReduceJob) => {

      singleThreadedSystem.run(j)
      val r = singleThreadedSystem.collect
      println(r)
      if (j.mapperPayload.headOption == None) true
      else {
        val reducedExpected = j.mapperPayload.flatMap(h => h.fileContents.map(x => x -> 1.0))
          .groupBy(x => x._1)
          .map(x => x._1 -> x._2.unzip._2)
          .map(x => x._1 -> x._2)
          .toList
          .map(x => x._1 -> x._2.reduce(_ + _))
          .sortBy(_._1)
        r.toList.sortBy(_._1).mkString("").equals(reducedExpected.mkString(""))

      }

    })
  }


  testWordCount


}

