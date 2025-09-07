package com.SmartCityAnalytics.models

case class Weather(
                    reading_id: String,
                    sensor_id: String,
                    timestamp: BigInt,
                    temperature: Option[Double],
                    humidity: Option[Double],
                    pressure: Option[Double],
                    wind_speed: Option[Double],
                    rainfall: Option[Double]
                  )