package com.sandbox

import java.util.concurrent.{Callable, ExecutorCompletionService, Executors}
import MapReduceJob._
/**
  * Created by amocanu on 1/27/2018.
  */
trait MapReduceSystem {
  def collect:Iterable[(String, String)]
  val sortOrder= Ordering.Tuple2(Ordering.String,Ordering.String.reverse).reverse
  def shutdown={}
  def run(job: MapReduceJob)
}

class MultiThreadedMapReduceSystem(parallelism: Int= 1) extends MapReduceSystem {
  val threadPool=Executors.newFixedThreadPool(parallelism)
  val mapperExecutor=new ExecutorCompletionService[MapperOutType](threadPool)
  val reducerExecutor=new ExecutorCompletionService[(String, Any)](threadPool)
  var reduced=Option.empty[Iterable[(String, String)]]

  class MapCallable(workUnit:String, job: Mapper) extends Callable[MapperOutType]{
    def call(): Iterable[(String, Any)] = job.map(workUnit)
  }
  class ReduceCallable(key:String, values:Iterable[(String, Any)], job: Reducer) extends Callable[(String, Any)]{
    def call(): (String, Any) = job.reduce(key,values)
  }

  def collect = reduced.getOrElse(List.empty[(String, String)])
  override def shutdown = threadPool.shutdown()

  def run(job: MapReduceJob) = {
    val mappedFutures=job.mapperPayload.flatMap(p=>p.fileContents.map(line=>{
      val mappedTuples=mapperExecutor.submit(new MapCallable(line,p.mapper))
      (mappedTuples,p.fileName)
    }))

    val mappedValues=mappedFutures.map{case (tuples,file)=> tuples.get.map(tuple=>tuple._1->(file->tuple._2))}
    val grouped=mappedValues.flatMap(identity).groupBy{case (k,_)=>k}.map{case (k,ls) => k->ls.unzip._2 }

    //merge step possible optimization: merge the results as they come instead of waiting for all to be done - semaphore
    val reducedFutures=grouped.map{case (k,vs) =>
      reducerExecutor.submit(new ReduceCallable(k,vs,job.reducer))
    }

    //possible optimization: parallel sort
    reduced=Some(reducedFutures.map(_.get).map(x=>x._1->x._2.toString).toArray.sortBy(x=>x._2->x._1)(sortOrder))
  }
}

class SingleThreadedMapReduceSystem extends MapReduceSystem {
  var reduced=Option.empty[Iterable[(String, String)]]
  override def collect = reduced.getOrElse(List.empty[(String, String)])

  def run(job: MapReduceJob) = {
    val mappedTuples=job.mapperPayload.flatMap(p=>p.fileContents.flatMap(line=>{
      p.mapper.map(line).map{case(k,v)=>k->(p.fileName->v)}
    }))

    val groupedTuples=mappedTuples.groupBy{case (k,_)=>k}.map{case (x,xs) => x->xs.unzip._2.toList }
    reduced=Some(groupedTuples.map{case (key,values)=>job.reducer.reduce(key, values)}
      .map{case(k,v)=>k->v.toString}.toList.sortBy(x=>x._2->x._1)(sortOrder))
  }
}
