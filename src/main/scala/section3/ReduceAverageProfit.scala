package section3

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._

object ReduceAverageProfit {

  /*
  flink run -c section3.ReduceAverageProfit target/scala-2.12/app.jar \
    --input file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section3/input/reduce \
  --output file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section3/output/reduce/averagereduceprofit.txt
*/
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val flinkParams = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(flinkParams)

    val source = env.readTextFile(flinkParams.get("input"))

    val reduced = source.map(line => {
      val splitted = line.split(",") //words = [{01-06-2018},{June},{Category5},{Bat}.{12}
      (splitted(1), splitted(2), splitted(3),splitted(4).toInt,1)
    })
      .keyBy(t => t._1)
      .reduce((current, previous) =>
        (current._1, current._2,current._3, current._4 +  previous._4, current._5 + previous._5)
      )

    reduced.writeAsText(flinkParams.get("output"),writeMode = WriteMode.OVERWRITE)

    env.execute("section 3 reduced example")






  }

}
