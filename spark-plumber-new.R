library(sparklyr); library(dplyr)

sc <- spark_connect(master = "local", version = "3.3.0")
spark_model <- ml_load(sc, path = "spark_model")

#* @post /predict
function(food_department, store_sales_in_millions_, store_cost_in_millions_, unit_sales_in_millions_, marital_status, gender, num_children_at_home, education, member_card, occupation, houseowner, avg_cars_at_home_approx_, avg_yearly_income, SRP, net_weight, recyclable_package, low_fat, units_per_case, store_type, store_state, store_sqft, sum_bars, promotion_keyword, bulk_mail, cash_register_handout, daily_paper, radio, TV, in_store_coupon, product_attachment, street_handout, sunday_paper){
  new_data <- data.frame(
    food_department = food_department,
    store_sales_in_millions_ = as.numeric(store_sales_in_millions_),
    store_cost_in_millions_ = as.numeric(store_cost_in_millions_),
    unit_sales_in_millions_ = as.numeric(unit_sales_in_millions_),
    marital_status = marital_status,
    gender = gender,
    num_children_at_home = as.numeric(num_children_at_home),
    education = education,
    member_card = member_card,
    occupation = occupation,
    houseowner = houseowner,
    avg_cars_at_home_approx_ = as.numeric(avg_cars_at_home_approx_),
    avg_yearly_income = avg_yearly_income,
    SRP = as.numeric(SRP),
    net_weight = as.numeric(net_weight),
    recyclable_package = recyclable_package,
    low_fat = low_fat,
    units_per_case = as.numeric(units_per_case),
    store_type = store_type,
    store_state = store_state,
    store_sqft = as.numeric(store_sqft),
    sum_bars = as.numeric(sum_bars),
    promotion_keyword = promotion_keyword,
    bulk_mail = bulk_mail,
    cash_register_handout = cash_register_handout,
    daily_paper = daily_paper,
    radio = radio,
    TV = TV,
    in_store_coupon = in_store_coupon,
    product_attachment = product_attachment,
    street_handout = street_handout,
    sunday_paper = sunday_paper,
    cost = NA
  )
  new_data_r <- copy_to(sc, new_data, overwrite = TRUE)
  ml_transform(spark_model, new_data_r) |>
    pull(prediction)
}
