#### Copies Data from VertiQ and saves it to Azure
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2021-01

message("--------------------------------------------------")
message(paste0(Sys.time(), " - Begin Script"))
### Call Libraries
suppressWarnings(library(odbc)) # Read to and write from SQL
suppressWarnings(library(keyring)) # Access stored credentials
suppressWarnings(library(glue)) # Safely combine code and variables
suppressWarnings(library(configr))
suppressWarnings(library(blastula))
suppressWarnings(library(htmlTable))

msg <- c("<style> table, th, td { border: 1px solid black; padding: 0 10px; } </style>",
         "<p>", format(Sys.time(), "%m/%d/%Y %X"), " - Begin Import Script</p>")

### BEGIN MAIN SCRIPT
assign("last.warning", NULL, envir = baseenv())
config <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/R/MEO/vertiq_copy/config/vertiq.config.yaml")
tables <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/R/MEO/vertiq_copy/config/vertiq.tables.yaml")
views <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/R/MEO/vertiq_copy/config/vertiq.views.yaml")
tables <- c(tables, views)

# VertiQ Connection
connV <- DBI::dbConnect(odbc::odbc(),
                        driver = "ODBC Driver 17 for SQL Server",
                        server = paste0("tcp:", config$from_server_address),
                        database = config$from_db,
                        uid = keyring::key_list(config$from_server)[["username"]],
                        pwd = keyring::key_get(config$from_server, keyring::key_list(config$from_server)[["username"]]),
                        Encrypt = "yes")
# Azure Connection
connA<- DBI::dbConnect(odbc::odbc(),
                       driver = "ODBC Driver 17 for SQL Server",
                       server = paste0("tcp:", config$to_server_address, ",1433"),
                       database = config$to_db,
                       uid = keyring::key_list(config$to_server)[["username"]],
                       pwd = keyring::key_get(config$to_server, keyring::key_list(config$to_server)[["username"]]),
                       Encrypt = "yes",
                       TrustServerCertificate = "yes",
                       Authentication = "ActiveDirectoryPassword")

qa <- data.frame(matrix(ncol = 4, nrow = 0))
colnames(qa) <- c("table", "source", "loaded", "difference")

for (i in 1:length(tables)) {
  ### get the fields and datatypes for the table and set table names
  tablevars <- tables[i]
  names(tablevars) <- c("vars")
  from_table <- names(tables[i])
  if (from_table %in% names(views)) {
    to_table <- paste0(config$table_prefix, "v", from_table)
    qa[i,1] <- paste0("v", from_table)
  } else {
    to_table <- paste0(config$table_prefix,from_table)
    qa[i,1] <- from_table
  }
  inc <- 10000
  
  qa[i,2] <- DBI::dbGetQuery(connV, 
                             glue::glue_sql("SELECT COUNT(*) AS cnt FROM 
                                            {`from_table`}", 
                                            .con = connV))
  
  select_vars <- tablevars$vars
  select_vars <- select_vars[order(unlist(select_vars), decreasing = F)]
  select_query <- "SELECT "
  
  ### add extra processing to the SELECT query based on field type
  for (v in 1:length(select_vars)) {
    ### remove tabs and new-lines from text fields and replace with semi-colon
    if (select_vars[v] == "NVARCHAR(MAX)" || select_vars[v] == "NVARCHAR(255)") {
      select_query <- paste0(select_query, 
                             "REPLACE(REPLACE(REPLACE(",
                             "[", names(select_vars[v]), "], ",
                             "CHAR(13), CHAR(59)), ",
                             "CHAR(10), CHAR(59)), ",
                             "CHAR(9), CHAR(59)) AS '", 
                             names(select_vars[v]), "'")
    } else {
      select_query <- paste0(select_query, 
                             "[", names(select_vars[v]), "]")
    }
    select_query <- paste0(select_query, ", ")
  }
  select_query <- paste0(select_query, "FROM ", 
                         config$from_db, ".",
                         config$from_schema, ".",
                         from_table)
  if ("Id" %in% names(select_vars)) {
    select_query <- paste0(select_query, " ORDER BY Id")
  }
  select_query <- gsub(", FROM", " FROM", select_query)
  
  data <- DBI::dbGetQuery(connV, select_query)
  data <- data[names(tablevars$vars)]
  
  ### Create new table
  d_stop <- as.integer(nrow(data) / inc)
  if (d_stop * inc < nrow(data)) { d_stop <- d_stop + 1 }
  
  for (d in 1:d_stop) {
    d_start <- ((d - 1) * inc) + 1
    d_end <- d * inc
    if (d_end > nrow(data)) { d_end <- nrow(data) }
    if ( d == 1) {
      dbWriteTable(connA, 
                   name = DBI::Id(schema = config$to_schema, table = to_table), 
                   value = data[d_start:d_end,],
                   overwrite = T, append = F,
                   field.types = unlist(tablevars$vars))
    }
    else {
      dbWriteTable(connA, 
                   name = DBI::Id(schema = config$to_schema, table = to_table), 
                   value = data[d_start:d_end,],
                   overwrite = F, append = T)
    }
  }
  qa[i,3] <- DBI::dbGetQuery(connA, 
                             glue::glue_sql("SELECT COUNT(*) AS cnt FROM 
                                            {`config$to_schema`}.{`to_table`}", 
                                            .con = connA))
  qa[i,4] <- qa[i,2] - qa[i,3]
}

DBI::dbExecute(connA,
               glue::glue_sql("
SELECT DecedentId,  Y.Col, IIF(X.Col = Y.Col, 1, 0) AS 'Val'
INTO #Temp1
FROM
(SELECT D.Id AS 'DecedentId', CONCAT('race_', LOWER(REPLACE(REPLACE(REPLACE(I.Description, ' ', ''), '.', ''), '/', ''))) AS 'Col'
	FROM {`config$to_schema`}.vertiq_Decedents_Items D INNER JOIN {`config$to_schema`}.vertiq_Items I ON D.Id2 = I.Id AND I.ListOfItems = 85 AND I.Deleted = 0) X
CROSS JOIN (SELECT CONCAT('race_', LOWER(REPLACE(REPLACE(REPLACE([Description], ' ', ''), '.', ''), '/', ''))) AS 'Col' 
	FROM {`config$to_schema`}.vertiq_Items WHERE ListOfItems = 85 AND Deleted = 0 GROUP BY CONCAT('race_', LOWER(REPLACE(REPLACE(REPLACE([Description], ' ', ''), '.', ''), '/', '')))) Y;

DECLARE @tcols nvarchar(max);
SELECT @tcols = COALESCE(@tcols + ', ', '') + Col + ' bit' FROM #Temp1 GROUP BY Col ORDER BY Col;

DECLARE @columns nvarchar(max);
SELECT @columns = COALESCE(@columns + ', ', '') + Col FROM #Temp1 GROUP BY Col ORDER BY Col;

DROP TABLE IF EXISTS {`config$to_schema`}.vertiq_vDecedents_Race;

DECLARE @csql nvarchar(max) = 'CREATE TABLE {`config$to_schema`}.vertiq_vDecedents_Race (DecedentId int, '+@tcols+')';
exec (@csql);

DECLARE @ssql nvarchar(max) = 'INSERT INTO {`config$to_schema`}.vertiq_vDecedents_Race (DecedentId,'+@columns+') SELECT DecedentId, '+@columns+' FROM #Temp1 PIVOT ( max(Val) for Col in ('+@columns+') ) AS pvt';
exec (@ssql);

DROP TABLE #Temp1;", .con = connA))

if(all(qa$difference == 0) == FALSE) {
  msg <- c(msg, 
           htmlTable(qa, rnames = F),
           "<p>", format(Sys.time(), "%m/%d/%Y %X"), " - Import Script Complete</p>",
           "<p>Warnings:</p>",
           "<p>", warnings(), "</p>")
  
  email <- compose_email(
    body = md(msg)
  )
  
  email %>%
    smtp_send(
      to = "jwhitehurst@kingcounty.gov",
      from = "jwhitehurst@kingcounty.gov",
      subject = paste0("AUTOMATED: MEO VertiQ Copy QA! ", format(Sys.Date(), "%m/%d/%Y")),
      credentials = creds_key("outlook")
    )
}

rm(list = ls())
