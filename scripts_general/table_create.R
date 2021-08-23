### FUNCTION TO CREATE TABLE BASED ON DATAFRAME OF VARS
table_create_f <- function(
  conn,
  schema,
  table,
  vars,
  overwrite = T) {
  #### if overwrite is TRUE then drop the table
  if (overwrite == T) {
    table_drop_f(conn, schema, table)
  }
  #### CREATE TABLE ####  
  create_code <- glue::glue_sql(
    "CREATE TABLE {`schema`}.{`table`} (
    {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(vars)`} {DBI::SQL(vars)}', 
    .con = conn), sep = ', \n'))}
    )", 
    .con = conn)
  DBI::dbExecute(conn, create_code)
}