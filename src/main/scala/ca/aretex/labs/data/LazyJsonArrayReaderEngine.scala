package ca.aretex.labs.data

import java.io.{InputStream, InputStreamReader}
import java.util.logging.{Level, Logger}

import ca.aretex.labs.data.jsonmodel.Person
import com.google.gson.stream.JsonReader
import com.google.gson.{Gson, GsonBuilder}


/**
 * This class read a large JsonArray in a lazy way using Scala Stream and tail-recursive functions.
 * This requires few memory
 * @param resourceRelativeFilepath the file is located in ${project.basedir}/src/main/resources
 */
class LazyJsonArrayReaderEngine(resourceRelativeFilepath: String) {

  val stream : InputStream = this.getClass().getResourceAsStream(resourceRelativeFilepath)

  def readStream(): Stream[Person] = {
    import Stream._
    
    try {
      val reader: JsonReader = new JsonReader(new InputStreamReader(stream, "UTF-8"))
      val gson: Gson = new GsonBuilder().create()

      reader.beginArray()

      def getPersons(hasNext: Boolean, agg: Stream[Person]): Stream[Person] ={
        if(!hasNext) agg
        else{
          val newagg = cons(gson.fromJson(reader, classOf[Person]), agg)
          getPersons(reader.hasNext(), newagg)
        }
      }
      
      val persons = getPersons(reader.hasNext(), Stream.empty[Person]).reverse

      // we close the inputstream
      reader.close()
      persons
    }
    catch {
      case ex: Throwable =>
        Logger.getLogger(this.getClass.getName()).log(Level.SEVERE, null, ex)
        Stream.empty[Person]
    }
  }
}
