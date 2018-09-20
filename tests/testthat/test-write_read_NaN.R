context("Test if NaN can be written and read back")
test_that("NaN write read test",{
  fn <- "../test_data/temp.parquet"
  n = c(NA, 1, NA)
  n = as.integer(n)
  s = c("aa", "bb", "cc")
  d = c(0.1,NA,NaN)
  e = c(FALSE,TRUE,FALSE)
  for (i in 1 : 20)
  {
    n <- c(n,n)
    s <- c(s,s)
    d <- c(d,d)
    e <- c(e,e)
  }
  df = data.frame(n,s,d,e)
  df$s <- as.character(df$s)
  a <- RParquet::rparquet_writer(df,fn)
  dff <- RParquet::rparquet_reader(fn)
  expect_equal(df,dff)
  expect_true(file.remove(fn))
})
