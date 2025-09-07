package com.ecommerce.models

case class CityZones(
                 zone_id: String,
                 zone_name: String,
                 zone_type: String,
                 population: BigInt,
                 area_km2: Double,
                 district: String
               )
