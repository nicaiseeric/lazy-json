package ca.aretex.labs.data.jsonmodel

import org.junit.Test
/**
 * Created by Choungmo Fofack on 5/4/17.
 */
class PersonTest {
  @Test
  def test() = {
    val person = Person(9999, false, "John Williams", null, Array(Child(16, "Shirley"), Child(11, "Jessica")))
    assert(
      "Person(9999, false, John Williams, List(), List(Child(16,Shirley), Child(11,Jessica)))"
        .equalsIgnoreCase(person.toString())
    )
  }
}
