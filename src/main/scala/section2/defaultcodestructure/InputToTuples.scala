package section2.defaultcodestructure

import org.apache.flink.api.common.functions.MapFunction

class InputToTuples extends MapFunction[String,(Int,String)] {
  override def map(t: String): (Int, String) = {
    val splitted: Array[String] = t.split(",")
    (splitted(0).trim.toInt, splitted(1).trim)
  }
}
