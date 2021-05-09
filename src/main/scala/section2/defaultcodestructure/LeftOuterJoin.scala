package section2.defaultcodestructure

import org.apache.flink.api.common.functions.{JoinFunction, MapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object LeftOuterJoin {

  /*
start im flink client
flink run -c section2.defaultcodestructure.LeftOuterJoin target/scala-2.12/app.jar \
--person file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section2/input/person.txt \
--location file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section2/input/location.txt \
--output file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section2/output/personlocationjoin/leftouterjoinresult.txt
 */


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val flinkArgs = ParameterTool.fromArgs(args)
    val input1Text = env.readTextFile(flinkArgs.get("person"))
    val input2Text = env.readTextFile(flinkArgs.get("location"))

    val personSet: DataSet[(Int, String)] = input1Text
      .map(new InputToTuples)

    val locationSet = input2Text
      .map(new InputToTuples)


    val joined: DataSet[(Int, String, String)] = personSet
      .leftOuterJoin(locationSet)
      .where(0)
      .equalTo(0).apply(
      //hier als lambda function
      //(person,location) => (person._1, person._2, location._2)

      //erstaunlich dass in Scala ein with für die join function wie in Java angeboten wird.
      //muss dem mal nachgehen, wie das im source code abgebildet ist
      //nein! s. oben - es ist anders implementiert - habe ich zuerst nicht verstanden

      new JoinFunction[(Int,String), (Int,String), (Int, String, String)] {
        override def join(person: (Int, String), location: (Int, String)): (Int, String, String) = {

          if (location == null)
            (person._1, person._2, "NULL")
          else
            (person._1, person._2, location._2)
        }
      }
    )

/*
Variante wäre

    //apply wird über JoinFunctionAssigner angewendet
    //und der syntactic sugra variante
    Funktion apply hat ein Argument also kann ich mit geschweiften Klammern schreiben
    Sebastian wendet einmal currying an, dem nochmals nachgehen
    //dem noch mal nachgehen
 */
    val joinedWithJoinFunctionAssigner: DataSet[(Int, String, String)] = personSet
      .leftOuterJoin(locationSet)
      .where(0)
      .equalTo(0) {
      //(person,location) => (person._1, person._2, location._2)

      //erstaunlich dass in Scala ein with für die join function wie in Java angeboten wird.
      //muss dem mal nachgehen, wie das im source code abgebildet ist

      new JoinFunction[(Int,String), (Int,String), (Int, String, String)] {
        override def join(person: (Int, String), location: (Int, String)): (Int, String, String) = {

          if (location == null)
            (person._1, person._2, "NULL")
          else
            (person._1, person._2, location._2)
        }
      }
    }




    joined.writeAsCsv(flinkArgs.get("output"),rowDelimiter = "\n", fieldDelimiter = ",", writeMode = WriteMode.OVERWRITE )

    env.execute("left outer join example")



  }

  class InputToTuples extends MapFunction[String,(Int,String)] {
    override def map(t: String): (Int, String) = {
      val splitted: Array[String] = t.split(",")
      (splitted(0).trim.toInt, splitted(1).trim)
    }
  }



}
