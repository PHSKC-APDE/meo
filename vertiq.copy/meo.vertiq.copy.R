#### Copies Data from VertiQ and saves it to Azure
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2022-08

message("--------------------------------------------------")
script_time <- Sys.time()
message("[", Sys.time(), "] Begin Script")
### Call Libraries
suppressMessages(suppressWarnings(library(odbc))) # Read to and write from SQL
suppressMessages(suppressWarnings(library(keyring))) # Access stored credentials
suppressMessages(suppressWarnings(library(glue))) # Safely combine code and variables
suppressMessages(suppressWarnings(library(dplyr)))
suppressMessages(suppressWarnings(library(stringr)))
suppressMessages(suppressWarnings(library(configr)))
suppressMessages(suppressWarnings(devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")))
suppressMessages(suppressWarnings(devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")))
suppressMessages(suppressWarnings(devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/meo/main/vertiq.copy/meo.vertiq.functions.R")))

msg <- c("<style> table, th, td { border: 1px solid black; padding: 0 10px; } </style>",
         "<p>", format(Sys.time(), "%m/%d/%Y %X"), " - Begin Import Script</p>")

### BEGIN MAIN SCRIPT
config <- suppressMessages(suppressWarnings(yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/meo/main/vertiq.copy/vertiq.config.yaml"))))
options(scipen = 999)
# VertiQ Connection
connV <- DBI::dbConnect(odbc::odbc(),
                        driver = "ODBC Driver 17 for SQL Server",
                        server = paste0("tcp:", config$from_server_address),
                        database = config$from_db,
                        uid = keyring::key_list(config$from_server)[["username"]],
                        pwd = keyring::key_get(config$from_server, keyring::key_list(config$from_server)[["username"]]),
                        Encrypt = "yes")
# Azure Connections
connA <- create_db_connection(config$to_server, interactive = F, prod = T)

tables <- get_table_list_f(connA, config)
inc <- config$copy_inc

round <- 1

repeat {
  message(glue("[{Sys.time()}] ROUND {round}"))
  message(glue("[{Sys.time()}] Tables to Copy: {nrow(tables)}"))
  
  for(t in 1:nrow(tables)) {
    # Reset Connections
    connV <- DBI::dbConnect(odbc::odbc(),
                            driver = "ODBC Driver 17 for SQL Server",
                            server = paste0("tcp:", config$from_server_address),
                            database = config$from_db,
                            uid = keyring::key_list(config$from_server)[["username"]],
                            pwd = keyring::key_get(config$from_server, keyring::key_list(config$from_server)[["username"]]),
                            Encrypt = "yes")
    connA <- create_db_connection(config$to_server, interactive = F, prod = T)
    
    table <- tables[t,]
    
    message(glue("[{Sys.time()}] {table$to_table} - {t} of {nrow(tables)}"))
    
    columns_pass <- check_table_columns_f(connV,
                                          table$from_schema,
                                          table$from_table,
                                          connA,
                                          table$to_schema,
                                          table$to_table)
    message(glue("[{Sys.time()}] {table$to_table} - Columns Check: {columns_pass}"))
    # Determine if table needs to be fully reset
    # Reasons include: 
    # 1. Different columns in source table
    # 2. The table is small and is easier to just rebuild than audit the rows
    # 3. It has been a long time (config$audit_table_days) since the table has been updated, so reset just to be safe
    if(columns_pass == FALSE || 
       (table$replace_table == TRUE && 
        difftime(Sys.time(), table$last_update_datetime, units = "hours") > config$audit_table_hours) || 
       difftime(Sys.time(), table$last_update_datetime, units = "days") > config$audit_table_days || 
       table$to_rows == 0) {
      reset_table <- TRUE
      rows_pass <- FALSE
      message(glue("[{Sys.time()}] {table$to_table} - Table has not been updated in {round(difftime(Sys.time(), table$last_update_datetime, units = 'hours'), 0)} hours ({round(difftime(Sys.time(), table$last_update_datetime, units = 'days'), 0)} days)"))
    } else {
      reset_table <- FALSE
      # Compares rows with source table to see if new rows are needed
      rows_pass <- check_table_rows_f(connV,
                                      table$from_schema,
                                      table$from_table,
                                      connA,
                                      table$to_schema,
                                      table$to_table)
      message(glue("[{Sys.time()}] {table$to_table} - Rows Check: {rows_pass}"))
    }
    
    vars <- get_table_columns_f(connV,
                                table$from_schema,
                                table$from_table)
    if(round == 1) {
      update_table_list_f(connA,
                          to_table = table$to_table,
                          column_name = "copy_start_datetime",
                          value = script_time)
    }
    if(round <= 2) {
      if(reset_table == TRUE || (rows_pass == FALSE && table$replace_table == TRUE) || table$to_rows == 0) {
        # Get all rows if table needs to be reset OR 
        # if the the number of rows differs with source table AND
        # the table is small and is easier to just rebuild than audit the rows
        table_exist <- DBI::dbGetQuery(connA,
                                       glue_sql("SELECT DISTINCT TABLE_NAME
		                                            FROM INFORMATION_SCHEMA.COLUMNS
		                                            WHERE TABLE_NAME = {table$to_table}",
                                                .con = connA))
        if(nrow(table_exist) > 0) {
          drop_rows <- get_table_rows_f(connA,                       
                                        schema_name = table$to_schema,
                                        table_name = table$to_table)
          if(drop_rows > 0) {
            copy_log_entry_f(connA,
                             table_name = table$to_table,
                             copy_rows = drop_rows * -1)
            message(glue("[{Sys.time()}] {table$to_table} - Deleting Rows: {drop_rows}"))
          }
          # Have to manually DROP TABLE due to dbExistsTable being case-sensitive, may remove later
          DBI::dbExecute(connA, 
                         glue::glue_sql("DROP TABLE {`table$to_schema`}.{`table$to_table`}",
                                        .con = connA))
        }
        update_table_list_f(connA,
                            to_table = table$to_table,
                            column_name = "to_rows",
                            value = 0)
        
        table_vars <- list()
        for(v in 1:nrow(vars)) {
          table_vars[vars[v,1]] <- vars[v,2]
        }
        message(glue("[{Sys.time()}] {table$to_table} - Rebuilding Table"))
        suppressMessages(create_table(connA,
                                      server = config$to_server,
                                      to_schema = table$to_schema,
                                      to_table = table$to_table,
                                      vars = table_vars))
        
        message(glue("[{Sys.time()}] {table$to_table} - Copying All Rows from Source"))
        copy_data_f(from_conn = connV,
                    from_schema = table$from_schema,
                    from_table = table$from_table,
                    to_conn = connA,
                    to_schema = table$to_schema,
                    to_table = table$to_table,
                    vars = vars,
                    inc = config$copy_inc,
                    all_rows = T,
                    audit_hours = config$audit_table_hours * 2)
      } else if(table$replace_updated == TRUE) {
        # Get updated rows by updated datetime or connecting to updated CaseNums and other IDs
        message(glue("[{Sys.time()}] {table$to_table} - Copying Rows from Source Based on Updated CaseNums and IDs"))
        copy_data_f(from_conn = connV,
                    from_schema = table$from_schema,
                    from_table = table$from_table,
                    to_conn = connA,
                    to_schema = table$to_schema,
                    to_table = table$to_table,
                    vars = vars,
                    inc = config$copy_inc,
                    updated_rows = T,
                    audit_hours = config$audit_table_hours * 2,
                    id_column = table$id_column)
      } else if(table$replace_keys == TRUE) {
        # Get only new rows based on audit_column (datetime based)
        message(glue("[{Sys.time()}] {table$to_table} - Copying Rows from Source Based on Table Keys"))
        copy_data_f(from_conn = connV,
                    from_schema = table$from_schema,
                    from_table = table$from_table,
                    to_conn = connA,
                    to_schema = table$to_schema,
                    to_table = table$to_table,
                    vars = vars,
                    inc = config$copy_inc,
                    keyed_rows = T,
                    audit_hours = config$audit_table_hours * 2)
      }
      
      create_table_index_f(connA,
                           schema_name = table$to_schema,
                           table_name = table$to_table)
    } else {
      message(glue("[{Sys.time()}] {table$to_table} - Rows Do Not Match. Copying Rows from Source Based on Table Keys"))
      copy_data_f(from_conn = connV,
                  from_schema = table$from_schema,
                  from_table = table$from_table,
                  to_conn = connA,
                  to_schema = table$to_schema,
                  to_table = table$to_table,
                  vars = vars,
                  inc = config$copy_inc,
                  keyed_rows = T,
                  audit_hours = config$audit_table_hours * 2)  
    }
  } 
  
  message(glue("[{Sys.time()}] All Tables Copied"))
  # Check for missing row
  suppressWarnings(rm(diff))
  diff <- get_table_diff_list_f(connA, config)
  if (nrow(diff) == 0 || round > 5) {
    break
  } else {
    tables <- get_table_list_f(connA, config)
    tables <- suppressMessages(dplyr::inner_join(tables, diff))
    round <- round + 1
  }
}

options(scipen = 0)
message(glue("[{Sys.time()}] Script Completed: {round(difftime(Sys.time(), script_time, units = 'mins'), 1)} minutes"))

rm(list = ls())