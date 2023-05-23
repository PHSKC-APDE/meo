get_table_list_f <- function(conn, 
                             config) {
  results <- DBI::dbGetQuery(conn,
                             glue::glue_sql("SELECT 
                                              [from_schema], [from_table], [from_rows], 
                                              [to_schema], [to_table], [to_rows], 
                                              [replace_table], [replace_keys], 
                                              [replace_updated], [last_update_datetime], [id_column]
                                            FROM {`config$to_schema`}.{`config$table_list`}                                             
                                            ORDER BY [to_table]",
                                            .con = conn))
  return(results)
}

get_table_diff_list_f <- function(conn, 
                                  config) {
  results <- DBI::dbGetQuery(conn,
                             glue::glue_sql("SELECT [to_table]
                                            FROM {`config$to_schema`}.{`config$table_list`} 
                                            WHERE [from_rows] <> [to_rows]
                                            ORDER BY [to_table]",
                                            .con = conn))
  return(results)
}

get_table_columns_f <- function(conn, 
                                schema_name, 
                                table_name) {
  results <- DBI::dbGetQuery(conn,
                             glue::glue_sql("SELECT 
                                            C.COLUMN_NAME AS 'column_name', 
                                            CONCAT(UPPER(C.DATA_TYPE), 
                                              IIF(C.CHARACTER_MAXIMUM_LENGTH IS NULL, '', 
                                                IIF(C.CHARACTER_MAXIMUM_LENGTH > 0, CONCAT('(', C.CHARACTER_MAXIMUM_LENGTH, ')'), '(MAX)')),
                                                IIF(ISNULL(C.NUMERIC_SCALE, 0) = 0, '', CONCAT('(', C.NUMERIC_PRECISION, ',', C.NUMERIC_SCALE, ')'))) AS 'column_type', 
                                            IIF(K.COLUMN_NAME IS NULL, 0, 1) AS 'key' 
                                            FROM INFORMATION_SCHEMA.COLUMNS C 
                                            LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE K 
                                              ON C.TABLE_CATALOG = K.TABLE_CATALOG 
                                                AND C.TABLE_SCHEMA = K.TABLE_SCHEMA 
                                                AND C.TABLE_NAME = K.TABLE_NAME 
                                                AND C.COLUMN_NAME = K.COLUMN_NAME 
                                            WHERE C.TABLE_SCHEMA = {schema_name} AND C.TABLE_NAME = {table_name}
                                            GROUP BY C.COLUMN_NAME, C.DATA_TYPE, C.CHARACTER_MAXIMUM_LENGTH, C.NUMERIC_SCALE, C.NUMERIC_PRECISION, K.COLUMN_NAME, C.ORDINAL_POSITION 
                                            ORDER BY C.ORDINAL_POSITION ASC",
                                            .con = conn))
  return(results)
}

check_table_columns_f <- function(from_conn,
                                  from_schema,
                                  from_table,
                                  to_conn,
                                  to_schema,
                                  to_table) {
  fc <- get_table_columns_f(from_conn,
                            from_schema,
                            from_table) %>% dplyr::select(column_name, column_type)
  tc <- get_table_columns_f(to_conn,
                            to_schema,
                            to_table) %>% dplyr::select(column_name, column_type)
  col_diff_f <- suppressMessages(dplyr::anti_join(fc, tc))
  col_diff_t <- suppressMessages(dplyr::anti_join(tc, fc))
  if(nrow(col_diff_f) + nrow(col_diff_t) == 0) {
    results <- TRUE
  } else {
    results <- FALSE
  }
  return(results)
}

get_table_rows_f <- function(conn, 
                             schema_name, 
                             table_name) {
  results <- DBI::dbGetQuery(conn,
                             glue::glue_sql("SELECT COUNT(*)
                                            FROM {`schema_name`}.{`table_name`}",
                                            .con = conn))
  return(as.numeric(results))
}

check_table_rows_f <- function(from_conn,
                               from_schema,
                               from_table,
                               to_conn,
                               to_schema,
                               to_table) {
  fr <- get_table_rows_f(from_conn,
                         from_schema,
                         from_table)
  tr <- get_table_rows_f(to_conn,
                         to_schema,
                          to_table)
  if(fr == tr) {
    results <- TRUE
  } else {
    results <- FALSE
  }
  return(results)
}

create_table_index_f <- function(conn,
                                schema_name,
                                table_name) {
  results <- DBI::dbGetQuery(conn,
                             glue::glue_sql("SELECT I.* 
                                            FROM sys.indexes I 
                                            INNER JOIN sys.tables T ON I.object_id = T.object_id 
                                            INNER JOIN sys.schemas S ON T.schema_id = S.schema_id 
                                            WHERE T.name = {table_name} 	
                                              AND S.name = {schema_name}
                                              AND I.type_desc = 'CLUSTERED COLUMNSTORE'",
                                            .con = conn))
  if(nrow(results) == 0)   {
    index_name <- paste0("idx_css_", tolower(schema_name), "_", tolower(table_name))
    DBI::dbExecute(conn,
                   glue::glue_sql("CREATE CLUSTERED COLUMNSTORE INDEX {`index_name`} ON 
                                    {`schema_name`}.{`table_name`}",
                                  .con = conn))
    message(glue("[{Sys.time()}] {table$to_table} - Index Created: {index_name}"))
  }
}

copy_data_f <- function(from_conn,
                        from_schema,
                        from_table,
                        to_conn,
                        to_schema,
                        to_table,
                        vars,
                        inc = 10000,
                        all_rows = F,
                        keyed_rows = F,
                        updated_rows = F,
                        audit_hours,
                        id_column = NA) {
  # Set default function to replace entire table
  if(all_rows == F && keyed_rows == F && updated_rows == F) { 
      all_rows <- T 
  }
  select_vars <- list()
  for(v in 1:nrow(vars)) {
    select_vars[vars[v,1]] <- vars[v,2]
  }
  select_vars <- select_vars[order(unlist(select_vars), decreasing = F)]
  select_query <- "SELECT "
  
  ### add extra processing to the SELECT query based on field type
  for (v in 1:length(select_vars)) {
    ### remove tabs and new-lines from text fields and replace with semi-colon
    if (grepl("CHAR\\(", select_vars[[v]])) {
      select_query <- paste0(select_query, 
                             "REPLACE(REPLACE(REPLACE(REPLACE(",
                             "A.[", names(select_vars[v]), "], ",
                             "CHAR(13), CHAR(59)), ",
                             "CHAR(10), CHAR(59)), ",
                             "CHAR(9), CHAR(59)), ",
                             "NCHAR(8217), CHAR(59)) AS '", 
                             names(select_vars[v]), "'")
    } else {
      select_query <- paste0(select_query, 
                             "A.[", names(select_vars[v]), "]")
    }
    select_query <- paste0(select_query, ", ")
  }
  select_query <- paste0(select_query, "FROM ")
  select_query <- paste0(gsub(", FROM", " FROM", select_query), from_schema, ".", from_table, " A ")
  

  if(updated_rows == TRUE) {
    # Create WHERE expression and add to SELECT query (Cannot use the Audit Tables or Missing Keys)
    last_copy <- get_last_copy_f(conn = to_conn,
                                 table_name = to_table)
    if(from_schema == "dbo") {
      ids <- DBI::dbGetQuery(from_conn,
                             glue_sql("SELECT DISTINCT [TableId] 
                                    FROM (SELECT TOP (50000) 
                                        [TableName],                              
                                        [TableId],                              
                                        [dte],                              
                                        DATEDIFF(hour, [dte], {last_copy}) AS 'UpdateDiff'                     
                                      FROM {`from_schema`}.[Audits]                                                                   
                                      WHERE [TableName] IS NOT NULL                                                                   
                                      ORDER BY [dte] DESC) A 
                                    WHERE [TableName] = {from_table} 
                                      AND [UpdateDiff] < {audit_hours} 
                                    ORDER BY [TableId]",
                                      .con = from_conn))
      updated_var <- id_column
    } else if(nrow(vars[which(vars$column_name == 'DeathRecord_ID'), ]) > 0) {
      updated_var <- 'DeathRecord_ID'
      ids <- DBI::dbGetQuery(from_conn,
                             glue_sql("SELECT [DeathRecord_ID] 
                                      FROM {`from_schema`}.[DeathRecord]
                                      WHERE DATEDIFF(hour, [ModifiedDateTime], {last_copy}) < {audit_hours}
                                      GROUP BY [DeathRecord_ID] 
                                      ORDER BY [DeathRecord_ID]",
                                      .con = from_conn))
    } else if(nrow(vars[which(vars$column_name == 'CaseNum'), ]) > 0) {
      updated_var <- 'CaseNum'
      ids <- DBI::dbGetQuery(from_conn,
                             glue_sql("SELECT DISTINCT [CaseNum] 
                                    FROM (SELECT TOP (50000) 
                                        [CaseNumber] AS 'CaseNum',                              
                                        [dte],                              
                                        DATEDIFF(hour, [dte], {last_copy}) AS 'UpdateDiff'                     
                                      FROM {`from_schema`}.[Audits]                                                                   
                                      ORDER BY [dte] DESC) A 
                                    WHERE [UpdateDiff] < {audit_hours} 
                                    ORDER BY [CaseNum]",
                                      .con = from_conn))
    }
    
    # Delete any data in the to_table that has been updated before loading new data
    rows_delete <- DBI::dbExecute(to_conn,
                                  glue_sql("DELETE FROM {`to_schema`}.{`to_table`}
                            WHERE {`updated_var`} IN({DBI::SQL(glue::glue_collapse(glue_sql('{ids[, 1]}', .con = to_conn), sep = ', '))})",
                                           .con = to_conn))
    select_query <- paste0(select_query,     
                           glue_sql(" WHERE {`updated_var`} IN({DBI::SQL(glue::glue_collapse(glue_sql('{ids[, 1]}', .con = from_conn), sep = ', '))})",
                                    .con = from_conn))
    # Add copy log entry for deleted rows and update table list to_rows
    if(rows_delete > 0) { 
      message(glue("[{Sys.time()}] {table$to_table} - Deleting Rows: {rows_delete}"))
      copy_log_entry_f(to_conn,                     
                       table_name = to_table,                     
                       copy_rows = rows_delete * -1) 
      update_table_list_f(to_conn,
                          to_table = to_table,
                          column_name = "to_rows",
                          value = get_table_rows_f(to_conn,
                                                   schema_name = to_schema,
                                                   table_name = to_table))
    }
    if(nrow(ids) > 0) {
      # Get data from the from_table and update table list
      message(glue("[{Sys.time()}] {table$to_table} - Begin Copying Data to Memory"))
      from_rows <- get_table_rows_f(from_conn,
                                    from_schema,
                                    from_table)
      from_data <- DBI::dbGetQuery(from_conn, select_query)
      try(DBI::dbRemoveTable(from_conn, "##tempKeys", temporary = T), silent = T)
      update_table_list_f(to_conn,
                          to_table = to_table,
                          column_name = "from_rows",
                          value = from_rows)
    } else {
      from_data <- as.data.frame(matrix(ncol = 1, nrow = 0))
    }
  } else if(keyed_rows == TRUE) {

  # Get the keys of the data in the to_table and compare with the from_table to only load new data 
  # Delete any data in the to_table that is no longer in the from_table
    keys <- vars %>%
      dplyr::filter(key == 1)
    if(nrow(keys) == 0) {
      keys <- vars[vars$column_name %in% c('DeathRecord_ID', 'CaseNum'), ]
    }
    key_vars <- list()
    for(v in 1:nrow(keys)) {
      key_vars[keys[v,1]] <- keys[v,2]
    }
    
    keys <- keys %>% dplyr::select(column_name)
    
    from_keys <- DBI::dbGetQuery(from_conn, 
                              glue_sql("SELECT {DBI::SQL(glue::glue_collapse(glue_sql('{`keys[, 1]`}', .con = to_conn), sep = ', '))}, COUNT(*) AS 'CNT'
                                       FROM {`from_schema`}.{`from_table`}
                                       GROUP BY {DBI::SQL(glue::glue_collapse(glue_sql('{`keys[, 1]`}', .con = to_conn), sep = ', '))}"
                                       , .con = from_conn))
    to_keys <- DBI::dbGetQuery(to_conn, 
                               glue_sql("SELECT {DBI::SQL(glue::glue_collapse(glue_sql('{`keys[, 1]`}', .con = to_conn), sep = ', '))}, COUNT(*) AS 'CNT'
                                        FROM {`to_schema`}.{`to_table`}
                                        GROUP BY {DBI::SQL(glue::glue_collapse(glue_sql('{`keys[, 1]`}', .con = to_conn), sep = ', '))}"
                                        , .con = to_conn))
    keys_delete <- suppressMessages(dplyr::anti_join(to_keys, from_keys))
    
    if(nrow(keys_delete) > 0) {
      keys_delete <- as.data.frame(keys_delete[, keys$column_name])
      names(keys_delete) <- keys$column_name
      try(DBI::dbRemoveTable(to_conn, "##tempKeys", temporary = T), silent = T)
      DBI::dbWriteTable(to_conn,
                        name = "##tempKeys",
                        vars = key_vars,
                        value = keys_delete)
      rows_delete <- DBI::dbExecute(to_conn,
                     glue_sql("DELETE A FROM {`to_schema`}.{`to_table`} A
                                INNER JOIN ##tempKeys B ON {DBI::SQL(glue::glue_collapse(glue_sql('ISNULL(A.{`names(keys_delete)`}, 0) = ISNULL(B.{`names(keys_delete)`}, 0)', .con = to_conn), sep = ' AND '))}",
                              .con = to_conn))   
      try(DBI::dbRemoveTable(to_conn, "##tempKeys", temporary = T), silent = T)
      
      # Add entry to copy log to show rows deleted
      message(glue("[{Sys.time()}] {table$to_table} - Deleting Rows: {rows_delete}"))
      copy_log_entry_f(to_conn,
                       table_name = to_table,
                       copy_rows = rows_delete * -1)
      # Update table list after deleting rows in the to_table
      update_table_list_f(to_conn,
                          to_table = to_table,
                          column_name = "to_rows",
                          value = get_table_rows_f(to_conn,
                                                   schema_name = to_schema,
                                                   table_name = to_table))
    }
    
    from_keys <- DBI::dbGetQuery(from_conn, 
                                 glue_sql("SELECT {DBI::SQL(glue::glue_collapse(glue_sql('{`keys[, 1]`}', .con = to_conn), sep = ', '))}, COUNT(*) AS 'CNT'
                                       FROM {`from_schema`}.{`from_table`}
                                       GROUP BY {DBI::SQL(glue::glue_collapse(glue_sql('{`keys[, 1]`}', .con = to_conn), sep = ', '))}"
                                          , .con = from_conn))
    to_keys <- DBI::dbGetQuery(to_conn, 
                               glue_sql("SELECT {DBI::SQL(glue::glue_collapse(glue_sql('{`keys[, 1]`}', .con = to_conn), sep = ', '))}, COUNT(*) AS 'CNT'
                                        FROM {`to_schema`}.{`to_table`}
                                        GROUP BY {DBI::SQL(glue::glue_collapse(glue_sql('{`keys[, 1]`}', .con = to_conn), sep = ', '))}"
                                        , .con = to_conn))
    keys_append <- suppressMessages(dplyr::anti_join(from_keys, to_keys))

    if(nrow(keys_append) > 0) {
      keys_append <- as.data.frame(keys_append[, keys$column_name])
      names(keys_append) <- keys$column_name
      try(DBI::dbRemoveTable(from_conn, "##tempKeys", temporary = T), silent = T)
      DBI::dbWriteTable(from_conn,
                        name = "##tempKeys",
                        vars = key_vars,
                        value = keys_append)
      select_query <- paste0(select_query,
                             glue_sql("INNER JOIN ##tempKeys B 
                                      ON {DBI::SQL(glue::glue_collapse(glue_sql('ISNULL(A.{`names(keys_append)`}, 0) = ISNULL(B.{`names(keys_append)`}, 0)', .con = from_conn), sep = ' AND '))}",
                                      .con = from_conn))
      
        # Get data from the from_table and update table list
        message(glue("[{Sys.time()}] {table$to_table} - Begin Copying Data to Memory"))
        from_rows <- get_table_rows_f(from_conn,
                                      from_schema,
                                      from_table)
        from_data <- DBI::dbGetQuery(from_conn, select_query)
        try(DBI::dbRemoveTable(from_conn, "##tempKeys", temporary = T), silent = T)
        update_table_list_f(to_conn,
                            to_table = to_table,
                            column_name = "from_rows",
                            value = from_rows)
    } else {
      from_data <- as.data.frame(matrix(ncol = 1, nrow = 0))
    }
  } else if(all_rows == T) {
    # Get data from the from_table and update table list
    message(glue("[{Sys.time()}] {table$to_table} - Begin Copying Data to Memory"))
    from_rows <- get_table_rows_f(from_conn,
                                  from_schema,
                                  from_table)
    from_data <- DBI::dbGetQuery(from_conn, select_query)
    try(DBI::dbRemoveTable(from_conn, "##tempKeys", temporary = T), silent = T)
    update_table_list_f(to_conn,
                        to_table = to_table,
                        column_name = "from_rows",
                        value = from_rows)
  } 
  
  ### Load from_data into the to_table in designated increments
  message(glue("[{Sys.time()}] {table$to_table} - Begin Copying Rows: {nrow(from_data)} in Increments of {inc} Rows"))
  if(nrow(from_data) > 0) {
    d_stop <- as.integer(nrow(from_data) / inc)
    if (d_stop * inc < nrow(from_data)) { d_stop <- d_stop + 1 }
  
    for (d in 1:d_stop) {
      d_start <- ((d - 1) * inc) + 1
      d_end <- d * inc
      if (d_end > nrow(from_data)) { d_end <- nrow(from_data) }
      message(glue("[{Sys.time()}] {table$to_table} - Copying Rows: {d_start} to {d_end} of {nrow(from_data)} Rows"))
      to_data <- as.data.frame(from_data[d_start:d_end,])
      names(to_data) <- names(from_data)
      dbWriteTable(to_conn, 
                  name = DBI::Id(schema = to_schema, table = to_table), 
                  value = to_data,
                  append = T)
      # Update table list after each increment is loaded
      update_table_list_f(to_conn,
                          to_table = to_table,
                          column_name = "to_rows",
                          value = get_table_rows_f(to_conn,
                                                   schema_name = to_schema,
                                                  table_name = to_table))
    }
  }
  # Add entry to copy log once all data is loaded
  message(glue("[{Sys.time()}] {table$to_table} - Copying Rows Completed: {nrow(from_data)} Rows Copied"))
  copy_log_entry_f(to_conn,
                   table_name = to_table,
                   copy_rows = nrow(from_data))
}


copy_log_entry_f <- function(conn,
                             table_name,
                             copy_rows) {
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO [meo].[zCopyLog]
                          (table_name, copy_rows, copy_datetime)                           
                          VALUES
                          ({table_name}, {copy_rows}, {format(Sys.time(), format='%Y-%m-%d %H:%M:%S')})",
                          .con = conn))
}

get_last_copy_f <- function(conn,
                            table_name) {
  last_copy <- DBI::dbGetQuery(conn,
                 glue_sql("SELECT MAX([copy_datetime]) AS 'last_copy' 
                          FROM [meo].[zCopyLog] 
                          WHERE table_name = {table_name} AND copy_rows > 0",
                          .con = conn))
  if(nrow(last_copy) > 0) {
    return(last_copy[1,1])
  }
  return(0)
}

update_table_list_f <- function(conn,
                                to_table,
                                column_name,
                                value,
                                copy_rows) {
  DBI::dbExecute(conn,
                 glue_sql("UPDATE [meo].[zTableList]
                          SET {`column_name`} = {format(value, format='%Y-%m-%d %H:%M:%S')}, [last_update_datetime] = {format(Sys.time(), format='%Y-%m-%d %H:%M:%S')}
                          WHERE [to_table] = {to_table}",
                          .con = conn))
}

