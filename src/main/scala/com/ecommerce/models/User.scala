package com.ecommerce.models

case class User(
                 user_id: String,
                 age: BigInt,
                 annual_income: Double,
                 city: String,
                 customer_segment: String,
                 preferred_categories: List[String],
                 registration_date: String
               )
