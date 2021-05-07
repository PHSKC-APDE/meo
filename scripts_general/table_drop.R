### FUNCTION TO ALTER TABLE IF TABLE EXISTS
table_drop_f <- function(
  conn,
  schema,
  table) {
  #### DROP TABLE ####  
  if (DBI::dbExistsTable(conn, DBI::Id(schema = schema, table = table))) {
    DBI::dbExecute(conn, 
                   glue::glue_sql("DROP TABLE {`schema`}.{`table`}",
                                  .con = conn))
  }
}