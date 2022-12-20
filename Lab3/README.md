# Lab 3 - Stream processing with Apache Flink

## RideCleanisingExercise
#### Задание
The task of the "Taxi Ride Cleansing" exercise is to cleanse a stream of TaxiRide events by removing events that start or end outside of New York City.
#### Код
```scala
val rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxDelay, speed)))
val filteredRides = rides.filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
```     
#### Пояснение
В переменную rides получаем значение поездок, а с помощью функции filter и параметра isInNYC, уберем поездки, которые начинаются или заканчиваются в Нью-Йорке

## RidesAndFaresExercise
#### Задание
The goal of this exercise TaxiRide and TaxiFare is to join together the and records for each ride.
#### Код
```scala
class EnrichmentFunction extends RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {
    // Настроенное, управляемое состояние
    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide]))
    lazy val fareState: ValueState[TaxiFare] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare]))

    override def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val fare = fareState.value
      if (fare != null) {
        fareState.clear()
        out.collect((ride, fare))
      }
      else {
        rideState.update(ride)
      }
    }

    override def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val ride = rideState.value
      if (ride != null) {
        rideState.clear()
        out.collect((ride, fare))
      }
      else {
        fareState.update(fare)
      }
    }
  }
```     
#### Пояснение
Реализуем класс EnrichmentFunction, который наследуется от RichCoFlatMapFunction. В нем мы будем соединять пары <TaxiRide, TaxiFare> по ключу rideId. Для этого будем использовать функции flatMap1 и flatMap2 , которые принимают на вход параметры TaxiRide или TaxiFare соответственно. Если в поле класса содержится значение taxiFare или taxiRide, то применяется out: Collector с переданным набором из 2-ух элементов, иначе поданная на вход var записывается в поле класса.

## HourlyTipsExerxise
#### Задание
The task of the "Hourly Tips" exercise is to identify, for each hour, the driver earning the most tips. 
#### Код
```scala
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
```     
#### Пояснение
Нахождение водителя с максимальными чаевыми за чам выполняется с помощью нескольких параметров, а именно fares и заранее созданный hourlyTips. При использовании fares мы выбираем определенные параметры, о водителе, времени и т.д. и находим общее число чаевых для водителя. А далее при помощи timeWindowAll и maxBy находим наибольшее значение по 2-ум полям.

## ExpiringStateExercise
#### Задание
The goal for this exercise is to enrich TaxiRides with fare information.
#### Код
```scala
class EnrichmentFunction extends KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {
    // Настроенное, управляемое состояние
    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide]))
    lazy val fareState: ValueState[TaxiFare] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare]))

    override def processElement1(ride: TaxiRide,
                                 context: KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                                 out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val fare = fareState.value
      if (fare != null) {
        fareState.clear()
        context.timerService.deleteEventTimeTimer(ride.getEventTime)
        out.collect((ride, fare))
      }
      else {
        rideState.update(ride)
        // Как только появится водяной знак, мы можем перестать ждать
        context.timerService.registerEventTimeTimer(ride.getEventTime)
      }
    }

    override def processElement2(fare: TaxiFare,
                                 context: KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                                 out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val ride = rideState.value
      if (ride != null) {
        rideState.clear()
        context.timerService.deleteEventTimeTimer(ride.getEventTime)
        out.collect((ride, fare))
      }
      else {
        fareState.update(fare)
        // Как только появится водяной знак, мы можем перестать ждать соответствующей поездки
        context.timerService.registerEventTimeTimer(fare.getEventTime)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedCoProcessFunction[Long, TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#OnTimerContext,
                         out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if (fareState.value != null) {
        ctx.output(unmatchedFares, fareState.value)
        fareState.clear()
      }
      if (rideState.value != null) {
        ctx.output(unmatchedRides, rideState.value)
        rideState.clear()
      }
    }
  }
```     
#### Пояснение
В данном задании было необходимо исправить главную проблему второго задания. А именно, проблема заключалась в том, что данные пары искались бесконечно, пока не заканчивались данные. Поэтому для решения этой проблемы, теперь используется таймер. С его помощью, мы ищем пару значений, только определенной время, после этого, если мы не нашли пару, записываем значение в объекты с тегами.
