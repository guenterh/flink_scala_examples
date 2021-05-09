package section2.defaultcodestructure

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object DefaultStructure {

  /*
  run with flink client
  flink run -c section2.defaultcodestructure.DefaultStructure target/scala-2.12/app.jar \
  --input file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section1/input \
  --output file:///home/swissbib/environment/code/learning/flink/udemy/flink_course_udemy/data/section1/output/simplemap
   */

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val text: DataSet[String] = env.readTextFile(params.get("input"))
    val filtered = text.filter( _.startsWith("N"))

    val tokenized = filtered.map(new Tokenizer)

    //groupBy does not suport key selector funtion strange
    //val counts: AggregateDataSet[(String, Integer)] = tokenized.groupBy(t => t._1)
    val counts = tokenized.groupBy(0)
      .sum(1)

    if (params.has("output"))
      counts.writeAsCsv(params.get("output"),"\n"," ",writeMode = WriteMode.OVERWRITE)


    env.execute("csv simple map job")

  }


  class Tokenizer extends MapFunction [String, (String, Integer)] {
    override def map(t: String): (String, Integer) = (t, 1)
  }

  class MyKeySelector extends KeySelector [(String,Integer), String] {
    override def getKey(in: (String, Integer)): String = in._1
  }




}
