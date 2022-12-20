package com.ververica.flinktraining.exercises.datastream_scala.basics

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import com.ververica.flinktraining.exercises.datastream_java.utils.{ExerciseBase, GeoUtils, MissingSolutionException}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * The "Ride Cleansing" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed to the
 * standard out.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
object RideCleansingExercise extends ExerciseBase {
  def main(args: Array[String]) {
    // Параметры синтаксического анализа
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", ExerciseBase.pathToRideData)

    val maxDelay = 60 // Максимальная задержка события - 60 секунд
    val speed = 600   // Каждую секунду происходит event, продолжительность - 10 минут

    // Выполняем настройку среды
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    // Получаем поток данных о поездке на такси
    val rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxDelay, speed)))

    val filteredRides = rides
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))


    // Выводим результат
    printOrTest(filteredRides)

    // Запускаем отчистку
    env.execute("Taxi Ride Cleansing")
  }

}
