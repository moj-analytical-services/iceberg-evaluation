get_dummy_deltas <- function(employees_path) {
  employees <- read.csv(employees_path, nrows = 10)

  # Create deleted flag
  employees$record_deleted <- FALSE
  employees$record_deleted <- as.logical(employees$record_deleted)

  # Cast to new int cols
  int_cols <- c("employee_id", "department_id", "manager_id")
  employees[int_cols] <- lapply(employees[int_cols], as.integer)

  # Cast to new string cols
  str_cols <- c("sex", "forename", "surname")
  employees[str_cols] <- lapply(employees[str_cols], as.character)

  # Let's split up the data and make some changes
  day1 <- employees[employees$employee_id %in% c(1, 2, 3, 4, 5), ]
  day1 <- day1[order(day1$employee_id), ]

  day2 <- employees[employees$employee_id %in% c(5, 6, 7), ]
  day2[1, "department_id"] <- 2
  day2[1, "manager_id"] <- 18
  day2 <- day2[order(day2$employee_id), ]

  day3 <- employees[employees$employee_id %in% c(1, 7, 9, 10, 11), ]
  day3$department_id <- 2
  day3$manager_id <- 5
  day3[1, "record_deleted"] <- TRUE
  day3[1, "department_id"] <- 1
  day3[1, "manager_id"] <- 17
  day3 <- day3[order(day3$employee_id), ]

  deltas <- list(
    day1 = day1,
    day2 = day2,
    day3 = day3
  )

  return(deltas)
}
