package ca.aretex.labs.data

import java.io.{InputStream, InputStreamReader}
import java.util.logging.{Level, Logger}

import com.google.gson.stream.JsonReader
import com.google.gson.{Gson, GsonBuilder}

import scala.annotation.tailrec
import scala.reflect.ClassTag


/**
 * This class read a large JsonArray in a lazy way using Scala Stream and tail-recursive functions.
 * This requires few memory
 * @param resourceRelativeFilepath the file is located in ${project.basedir}/src/main/resources
 */
class LazyJsonArrayReaderEngine[T:ClassTag](resourceRelativeFilepath: String) {

  val stream : InputStream = this.getClass.getResourceAsStream(resourceRelativeFilepath)

  def readStream(): Stream[T] = {
    import Stream._
    
    try {
      val reader: JsonReader = new JsonReader(new InputStreamReader(stream, "UTF-8"))
      val gson: Gson = new GsonBuilder().create()

      reader.beginArray()

      /**
       * this tail-rec function builds the stream of T's objects
       * it reuses the same memory stack to reduce memory consumption
       * @param hasNext indicates if an object can be parse from the JsonArray
       * @param agg contains the actual stream of objects found
       * @return
       */
      @tailrec
      def getPersons(hasNext: Boolean, agg: Stream[T]): Stream[T] ={
        if(!hasNext) agg
        else{
          val newagg = cons(gson.fromJson(reader, implicitly[ClassTag[T]].runtimeClass), agg)
          getPersons(reader.hasNext, newagg)
        }
      }
      
      val persons = getPersons(reader.hasNext, Stream.empty[T]).reverse

      // we close the inputstream
      reader.close()
      persons
    }
    catch {
      case ex: Throwable =>
        Logger.getLogger(this.getClass.getName).log(Level.SEVERE, null, ex)
        Stream.empty[T]
    }
  }
}
