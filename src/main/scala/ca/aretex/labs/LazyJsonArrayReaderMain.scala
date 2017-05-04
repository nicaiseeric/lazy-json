package ca.aretex.labs

import ca.aretex.labs.data.LazyJsonArrayReaderEngine

object LazyJsonArrayReaderMain {

  def main(args: Array[String]) {

    val filename = "/data/persons/large-jsonarray-data.json"

    val lazyJsonArrayReaderEngine = new LazyJsonArrayReaderEngine(filename)

    val ti = System.currentTimeMillis()
    println("Start reading in stream mode: " + ti)

    val persons = lazyJsonArrayReaderEngine.readStream()

    val tf = System.currentTimeMillis()
    println("Finish. Total time: " + (tf - ti))

    for(person <- persons){
      println(person)
    }
  }

}
