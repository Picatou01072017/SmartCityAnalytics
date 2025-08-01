package com.ecommerce.models


case class TimeFeatures(
                         hour: Int,
                         dayOfWeek: String,
                         month: String,
                         isWeekend: Int,
                         dayPeriod: String,
                         isWorkingHour: Int
                       )
