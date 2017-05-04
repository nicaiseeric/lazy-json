package ca.aretex.labs.data.jsonmodel

/**
 * Created by Choungmo Fofack on 5/4/17.
 */
/**
 * The file contain an array of objects of class Person
 * @param id a unique identifier
 * @param married a boolean that indicates if this person is married
 * @param name
 * @param sons
 * @param daughters
 */
case class Person(id: Int, married: Boolean, name: String, sons: Array[Child], daughters: Array[Child]){
  override def toString() = s"Person($id, $married, $name" +
      s", ${if(sons == null)List.empty[Child] else sons.toList}" +
      s", ${if(daughters == null)List.empty[Child] else daughters.toList})"
}
