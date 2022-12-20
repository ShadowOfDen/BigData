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
