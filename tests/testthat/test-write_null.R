w_fp <- "../test_data/temp.parquet"
create_null_df <- function(all) {
  n = 100
  a <- do.call(paste0, replicate(15, sample(LETTERS, n, TRUE), FALSE))
  df <- data.frame(A = sample(n:n*n, n, TRUE), B = a, C = runif(2))
  df <- as.data.frame(lapply(df, function(random_na) random_na[sample(c(TRUE, NA),
                    prob = c(0.85, 0.15),
                    size = length(random_na),
                    replace = TRUE) ]))
  if(all) {
    df[sample(1:ncol(df), 1)] <- NA
  }
  return(df)
}

context("test writer and reader in data frame with NA cells")
test_that("mixed nulls can be writen and read back",{
  df = create_null_df(FALSE)
  system.time(RParquet::rparquet_writer(df, w_fp))
  df$B <- as.character(df$B)
  system.time(p_df <- RParquet::rparquet_reader(w_fp))
  expect_equal(df, p_df)
  expect_true(file.remove(w_fp))
})

context("random null columns can be safely read & write")
test_that("null columns can be safely read & write",{
  df <- create_null_df(TRUE)
  system.time(RParquet::rparquet_writer(df, w_fp))
  system.time(p_df <- rparquet_reader(w_fp))
  expect_equal(df,p_df)
  expect_true(file.remove(w_fp))
})

context("test writer and reader with integer null")
test_that("integer null",{
  n = c(NA, NA, NA)
  n = as.integer(n)
  s = c("aa", "bb", "cc")
  d = c(0.1, NA, 0.3)
  e = c(FALSE,NA,FALSE)
  df = data.frame(n,s,d,e)
  rparquet_writer(df, w_fp)
  df$s <- as.character(df$s)
  p_df <- RParquet::rparquet_reader(w_fp)
  expect_equal(df, p_df)
  expect_true(file.remove(w_fp))
})

context("test writer and reader with double null")
test_that("double null",{
      n = c(1, 2, 3)
      s = c("aa", "bb", "cc")
      d = c(NA, NA, NA)
      d = as.double(d)
      e = c(FALSE,NA,FALSE)
      df = data.frame(n,s,d,e)
      rparquet_writer(df, w_fp)
      df$s <- as.character(df$s)
      p_df <- RParquet::rparquet_reader(w_fp)
      expect_equal(df, p_df)
      expect_true(file.remove(w_fp))
    })

context("test writer and reader with string null")
test_that("string null",{
  n = c(NA, NA, NA)
  n = as.character(n)
  s = c("aa", "bb", "cc")
  d = c(0.1,NA,0.3)
  df = data.frame(n,s,d)
  a <- rparquet_writer(df,w_fp)
  df$s <- as.character(df$s)
  df$n <- as.character(df$n)
  p_df <- RParquet::rparquet_reader(w_fp)
  expect_equal(df,p_df)
  expect_true(file.remove(w_fp))
})

test_that("Empty DataFrame Test",{
  df <- data.frame(c())
  msg <- "Expected : ncol(df) > 0"
  expect_error(rparquet_writer(df,w_fp),msg,fixed=TRUE)
})
