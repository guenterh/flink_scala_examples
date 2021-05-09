package section2.defaultcodestructure

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object WordCount10 {

  /*
  run with flink client
  flink run -c section2.defaultcodestructure.WordCount10 target/scala-2.12/app.jar \
  --flatinput file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section1/input \
  --flatoutput file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section1/output/flatmap/
   */

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)


    val flattext = env.readTextFile(params.get("flatinput"))

    val flatgropupby: GroupedDataSet[(String, Int)] = flattext.flatMap(new FlatMapTokenizer)
      .filter(_._1.startsWith("I"))
      .groupBy(0)


    val flatcounts = flatgropupby.sum(1)

    if (params.has("flatoutput"))
      flatcounts.writeAsCsv(params.get("flatoutput"),"\n"," ",writeMode = WriteMode.OVERWRITE)

    env.execute("csv flat map job")

  }

}

class FlatMapTokenizer extends FlatMapFunction[String,(String,Int)] {
  override def flatMap(t: String, collector: Collector[(String, Int)]): Unit = {
    t.split(" ").map(token => (token.trim,1)).foreach(collector.collect)
  }
}

