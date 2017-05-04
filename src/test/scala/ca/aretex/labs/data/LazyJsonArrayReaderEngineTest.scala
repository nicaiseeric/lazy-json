package ca.aretex.labs.data

import java.io.InputStream
import ca.aretex.labs.data.jsonmodel.Person
import org.junit.Test

/**
 * Created by Choungmo Fofack on 5/4/17.
 */
class LazyJsonArrayReaderEngineTest {

  val expectedSmallSize = 7
  val expectedLargeSize = 10000

  @Test
  def testReadStreamSmallFile() = {
    val relativeResourceFilepath = "/small_jsonarray_data.json"
    case class Color(color: String, value: String)
    
    val inputStream : InputStream = this.getClass.getResourceAsStream(relativeResourceFilepath)

    val lazyJsonEngine = new LazyJsonArrayReaderEngine[Color]()

    val colors = lazyJsonEngine.readStream(inputStream)

    assert(expectedSmallSize == colors.size)
  }

  @Test
  def testReadStreamLargeFile() = {
    val relativeResourceFilepath = "/large_jsonarray_data.json"

    val inputStream : InputStream = this.getClass.getResourceAsStream(relativeResourceFilepath)

    val lazyJsonEngine = new LazyJsonArrayReaderEngine[Person]()

    val persons = lazyJsonEngine.readStream(inputStream)

    assert(expectedLargeSize == persons.size)
  }
}
