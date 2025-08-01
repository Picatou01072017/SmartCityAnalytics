package com.ecommerce.models

case class Merchant(
                     merchant_id: String,
                     name: String,
                     category: String,
                     region: String,
                     commission_rate: Double,
                     establishment_date: String
                   )
