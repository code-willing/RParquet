library(data.table)
r_fp <- "../test_data/data.parquet"
w_fp <- "../test_data/temp.parquet"

set.random.na.dt <- function(dt, testNaNs = FALSE, ratio = 0.2){
  dt <- as.data.table(dt)
  for (col in names( dt)){
    cl = dt[, eval( parse( text = paste("class(",col,")")))]
    cl
    ix = sample(NROW(dt), NROW(dt) * ratio)
    if( cl %in% c("logical","integer","character")){
      dt[ ix, (col):= NA]
    }
    if( cl %in% c("numeric")){
      dt[ ix, (col):= NA]
      if( testNaNs){
        ix = sample(NROW(dt), NROW(dt) * ratio)
        dt[ ix, (col):= NaN]
      }
      ix = sample(NROW(dt), NROW(dt) * ratio * 0.5)
      dt[ ix, (col):= Inf]
      ix = sample(NROW(dt), NROW(dt) * ratio * 0.5)
      dt[ ix, (col):= -Inf]
    }
    cl2 = dt[, eval( parse( text = paste("class(",col,")")))]
    stopifnot( identical(cl, cl2))
  }
  return (dt)
}

context("Random Corner Case Test With NaN")
test_that("Random Corner Case Test With NaN",{
  x <- system.time(df <- RParquet::rparquet_reader(r_fp))
  df <- set.random.na.dt(df, testNaNs = TRUE)
  rparquet_writer(as.data.frame(df),w_fp)
  df2 <- rparquet_reader(w_fp)
  expect_equal(as.data.frame(df),df2)
  expect_true(file.remove(w_fp))
})

context("Random Corner Case Test Without NaN")
test_that("Random Corner Case Test Without NaN",{
  x <- system.time(df <- RParquet::rparquet_reader(r_fp))
  df <- set.random.na.dt(df)
  w_fp <- "temp.parquet"
  rparquet_writer(as.data.frame(df),w_fp)
  df2 <- rparquet_reader(w_fp)
  expect_equal(as.data.frame(df),df2)
  expect_true(file.remove(w_fp))
})
