### FUNCTION TO CREATE DATABASE CONNECTION
db_connect_f <- function(server, server_address = NA, db_name, auth = F) {

  if (auth == F) {
    conn <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = paste0("tcp:", server_address),
                           database = db_name,
                           uid = keyring::key_list(server)[["username"]],
                           pwd = keyring::key_get(server, keyring::key_list(server)[["username"]]),
                           Encrypt = "yes")
  } else {
    conn <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = paste0("tcp:", server_address),
                           database = db_name,
                           uid = keyring::key_list(server)[["username"]],
                           pwd = keyring::key_get(server, keyring::key_list(server)[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")
  }
  
  return(conn)
}