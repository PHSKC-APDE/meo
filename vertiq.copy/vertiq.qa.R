vertiq_qa_conn_check_f(
  conn, 
  config) 
{
  suppressWarnings(library(odbc)) # Read to and write from SQL
  suppressWarnings(library(keyring)) # Access stored credentials
  suppressWarnings(library(glue)) # Safely combine code and variables
  suppressWarnings(library(configr))
  config <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/GitHub/meo/config/vertiq.config.yaml")
  
  conn <- DBI::dbConnect(odbc::odbc(),
                          driver = "ODBC Driver 17 for SQL Server",
                          server = paste0("tcp:", config$from_server_address),
                          database = config$from_db,
                          uid = keyring::key_list(config$from_server)[["username"]],
                          pwd = keyring::key_get(config$from_server, keyring::key_list(config$from_server)[["username"]]),
                          Encrypt = "yes")
  
  results <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT COUNT(*) 
    FROM {`config$to_Schema`}.{`paste0(config$table_prefix, config$qa_table_copy)`}", 
    .con = conn))
  
}



