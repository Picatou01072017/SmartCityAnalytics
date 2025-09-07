package com.ecommerce.models

case class Sensors(
                sensor_id: String,
                sensor_type: String,
                zone_id: String,
                installation_date: String,
                latitude: Double ,
                longitude: Double,
                status: String
               )
