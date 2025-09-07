package com.SmartCityAnalytics.models

case class TrafficEvents(
                          event_id: String,
                          sensor_id: String,
                          timestamp: BigInt,
                          vehicle_count: Option[Int],
                          avg_speed: Option[Double],
                          traffic_density: Option[Double],
                          incident_type: String
                        )

