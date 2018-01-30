package com.sandbox

/**
  * Created by amocanu on 1/27/2018.
  */

object MapReduceJob {
  type MapperOutType = Iterable[(String, Any)]

  trait Mapper {
    def map(line: String): MapperOutType
  }

  trait Reducer {
    def reduce(key: String, values: Iterable[(String, Any)]): (String, Any)
  }

  case class MapperPayload(mapper: Mapper, fileName: String, fileContents: List[String])

  case class MapReduceJob(mapperPayload: List[MapperPayload], reducer: Reducer)

}