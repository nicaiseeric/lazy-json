package ca.aretex.labs.data.jsonmodel

import org.junit.Test

/**
 * Created by Choungmo Fofack on 5/4/17.
 */
class ChildTest {
  @Test
  def test(): Unit ={
    val expectedAge = 4
    val expectedName = "JN"

    val child = Child(4, "JN")

    assert(expectedAge == child.age)
    assert(expectedName == child.name)
    assert("Child(4,JN)".equalsIgnoreCase(child.toString))
  }

}
