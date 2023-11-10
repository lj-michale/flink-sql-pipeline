package org.turing.scala.flink.pipeline.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import scala.util.Random

/**
 * @descri: word stream pipeline
 *
 * @author: lj.michale
 * @date: 2023/11/10 17:02
 */
class WordSource extends SourceFunction[String] {

  private var is_running = true
  private val words = Array("hello", "world", "flink", "stream", "batch", "table", "sql")

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    while (is_running) {
      val index = Random.nextInt(words.size)
      sourceContext.collect(words(index))
      // 1秒
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    is_running = false
  }

}
