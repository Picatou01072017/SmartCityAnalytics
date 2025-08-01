package com.ecommerce.models

case class User(
                 user_id: String,
                 age: Int,
                 annual_income: Double,
                 city: String,
                 customer_segment: String,
                 preferred_categories: Seq[String],
                 registration_date: String
               )
