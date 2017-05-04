package ca.aretex.labs

import ca.aretex.labs.data.LazyJsonArrayReaderEngine
import ca.aretex.labs.data.jsonmodel.Person

object LazyJsonArrayReaderMain {

  def main(args: Array[String]) {

    val resourceRelativeFilepath = if(args.length > 0) args(0) else "/data/persons/large-jsonarray-data.json"

    val inputStream = this.getClass.getResourceAsStream(resourceRelativeFilepath)

    val lazyJsonArrayReaderEngine = new LazyJsonArrayReaderEngine[Person]()

    val ti = System.currentTimeMillis()
    println("Start reading in stream mode: " + ti)

    val persons = lazyJsonArrayReaderEngine.readStream(inputStream)

    val tf = System.currentTimeMillis()
    println("Finish. Total time: " + (tf - ti))

    for(person <- persons){
      println(person)
    }
  }

}
