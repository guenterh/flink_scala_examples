package section3

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object SocketExample {

  /*
    flink run -c section3.SocketExample target/scala-2.12/app.jar \
  --hostname localhost \
  --port 9999
   */

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(params)

    val socket = env.socketTextStream(hostname = params.get("hostname"), port = params.get("port").toInt)

    val splittedLines = socket.flatMap(line => {
      line.split(" ").map(term => (term.trim,1))
    })

    val sumUp = splittedLines
      .keyBy( t => t._1)
      .sum(1)

    sumUp.print()

    env.execute("first simple word count socket example")


  }

}
