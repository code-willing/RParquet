fp <- "../test_data/data.parquet"

context("consistent reader with random column selection")
test_that("random column selection",{
      system.time(r_df1 <- RParquet::rparquet_reader(fp))
      cols <- sort(sample(ncol(r_df1), ncol(r_df1)/2))
      system.time(r_df2 <- RParquet::rparquet_reader(fp,cols))
      expect_equal(r_df1[cols],r_df2)
    })


context("consistent reader with random row filter")
test_that("random filter",{
      system.time(r_df1 <- RParquet::rparquet_reader(fp))
      filter = sample(c(TRUE,FALSE), nrow(r_df1), TRUE)
      r_df1 = r_df1[filter, ]
      row.names(r_df1) <- NULL
      system.time(r_df2 <- RParquet::rparquet_reader(fp, filter = filter))
      row.names(r_df2) <- NULL
      expect_equal(sum(filter),nrow(r_df2))
      expect_equal(r_df1, r_df2)
    })

context("consistent reader by filtering all rows")
test_that("zero row filter",{
      system.time(r_df1 <- RParquet::rparquet_reader(fp))
      filter = c(FALSE)
      expect_equal(0,sum(filter))
      expect_error(RParquet::rparquet_reader(fp,filter = filter))
    })

context("consistent reader by selecting all rows(Not default argument)")
test_that("All row filter",{
      system.time(r_df1 <- RParquet::rparquet_reader(fp))
      filter = rep(TRUE, nrow(r_df1))
      system.time(r_df2 <- RParquet::rparquet_reader(fp,filter = filter))
      expect_equal(r_df1, r_df2)
    })
