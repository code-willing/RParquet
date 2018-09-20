context("test reader memory usage is optimal")


test_that("memory improve new", {
  rm(list = ls())
  tmp <- gc(reset = TRUE)
  df <- rparquet_reader('../test_data/data.parquet')
  x1 <- gc()
  dif <-  x1[4][1]-tmp[4][1]
  if(dif == 0) dif = 1;
  expect_true((x1[12][1]-tmp[12][1])/dif <3)
})


context("test writer memory usage is optimal")
test_that("memory improve new", {
  rm(list = ls())
  df <- rparquet_reader('../test_data/data.parquet')
  tmp <- gc(reset = TRUE)
  rparquet_writer(df, '../test_data/temp.parquet')
  x1 <- gc()
  dif <-  x1[4][1]-tmp[4][1]
  if(dif == 0) dif = 1;
  expect_true((x1[12][1]-tmp[12][1])/dif <3)
  expect_true(file.remove('../test_data/temp.parquet'))
})
