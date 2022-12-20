package com.ververica.flinktraining.exercises.datastream_scala.windows

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource
import com.ververica.flinktraining.exercises.datastream_java.utils.{ExerciseBase, MissingSolutionException}
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * The "Hourly Tips" exercise of the Flink training
  * (http://training.ververica.com).
  *
  * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
  * then from that stream, find the highest tip total in each hour.
  *
  * Parameters:
  * -input path-to-input-file
  *
  */
object HourlyTipsExercise {

  def main(args: Array[String]) {

    // Считываем параметры
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", ExerciseBase.pathToFareData)

    val maxDelay = 60 // Максимальная задержка события - 60 секунд
    val speed = 600   // Каждую секунду происходит event, продолжительность - 10 минут

    // Настройка среды потокового выполнения
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    // Запуск генерации данных
    val fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxDelay, speed)))

    // Общее количество чаевых в час от водителя
    val hourlyTips = fares
      .map((f: TaxiFare) => (f.driverId, f.tip))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .reduce(
        (f1: (Long, Float), f2: (Long, Float)) => { (f1._1, f1._2 + f2._2) },
        new WrapWithWindowInfo())

    // Максимальная сумма чаевых за каждый час
    val hourlyMax = hourlyTips
      .timeWindowAll(Time.hours(1))
      .maxBy(2)

    // Вывод результата
    printOrTest(hourlyMax)

    // Выполняем преобразование
    env.execute("Hourly Tips (scala)")
  }

  class WrapWithWindowInfo() extends ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[(Long, Float)], out: Collector[(Long, Long, Float)]): Unit = {
      val sumOfTips = elements.iterator.next()._2
      out.collect((context.window.getEnd(), key, sumOfTips))
    }
  }

}
