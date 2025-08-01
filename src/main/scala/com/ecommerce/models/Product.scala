package com.ecommerce.models

case class Product(
                    product_id: String,
                    name: String,
                    category: String,
                    price: Double,
                    merchant_id: String,
                    rating: Double,
                    stock: Int
                  )
