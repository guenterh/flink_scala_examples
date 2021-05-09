package section2.defaultcodestructure

import org.apache.flink.api.common.functions.{JoinFunction, MapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object JoinFunctionality {


  /*
  start im flink client
  flink run -c section2.defaultcodestructure.JoinFunctionality target/scala-2.12/app.jar \
  --person file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section2/input/person.txt \
  --location file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section2/input/location.txt \
  --output file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section2/output/personlocationjoin/joinresult.txt
   */

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val flinkArgs = ParameterTool.fromArgs(args)
    val input1Text = env.readTextFile(flinkArgs.get("person"))
    val input2Text = env.readTextFile(flinkArgs.get("location"))

    val personSet: DataSet[(Int, String)] = input1Text.map(new InputToTuples)
    val locationSet = input2Text.map(new InputToTuples)


    val joined: DataSet[(Int, String, String)] = personSet.join(locationSet).where(0).equalTo(0) {
      //(person,location) => (person._1, person._2, location._2)

      //erstaunlich dass in Scala ein with für die join function wie in Java angeboten wird.
      //muss dem mal nachgehen, wie das im source code abgebildet ist

      new JoinFunction[(Int,String), (Int,String), (Int, String, String)] {
        override def join(person: (Int, String), location: (Int, String)): (Int, String, String) =
          (person._1, person._2, location._2)
      }
    }


    joined.writeAsCsv(flinkArgs.get("output"),rowDelimiter = "\n", fieldDelimiter = ",", writeMode = WriteMode.OVERWRITE )

    env.execute("first joined example")

  }


}
