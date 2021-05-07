#### Checks for new columns in VertiQ Database ####
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2021-01

message("--------------------------------------------------")
message(paste0(Sys.time(), " - Begin Script"))
### Call Libraries
suppressWarnings(library(odbc)) # Read to and write from SQL
suppressWarnings(library(keyring)) # Access stored credentials
suppressWarnings(library(compare))
suppressWarnings(library(configr))
suppressWarnings(library(blastula))
suppressWarnings(library(htmlTable))
suppressWarnings(library(tidyverse)) # Manipulate data

### BEGIN MAIN SCRIPT
assign("last.warning", NULL, envir = baseenv())
config <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/R/MEO/vertiq_copy/config/vertiq.config.yaml")
tables <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/R/MEO/vertiq_copy/config/vertiq.tables.original.yaml")
views <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/R/MEO/vertiq_copy/config/vertiq.views.original.yaml")
tables <- c(tables, views)

# VertiQ Connection
connV <- DBI::dbConnect(odbc::odbc(),
                        driver = "ODBC Driver 17 for SQL Server",
                        server = paste0("tcp:", config$from_server_address),
                        database = config$from_db,
                        uid = keyring::key_list(config$from_server)[["username"]],
                        pwd = keyring::key_get(config$from_server, keyring::key_list(config$from_server)[["username"]]),
                        Encrypt = "yes")

qa <- data.frame(matrix(ncol = 4, nrow = 0))
for (t in 1:length(tables)) {
  tablevars <- tables[t]
  names(tablevars) <- c("vars")
  vars <- as.data.frame(names(tablevars$vars))
  names(vars) <- c("var")
  cols <- DBI::dbGetQuery(connV, 
                          paste0("SELECT COLUMN_NAME AS 'col' ",
                                 "FROM INFORMATION_SCHEMA.COLUMNS ",
                                 "WHERE TABLE_NAME = '", names(tables[t]), "'",
                                 "ORDER BY ORDINAL_POSITION"))
  comp <- compare(cols,vars,allowAll=TRUE)
  diff <- data.frame(lapply(1:ncol(cols),function(i)setdiff(cols[,i],comp$tM[,i])))
  colnames(diff) <- colnames(cols)
  if (nrow(diff) > 0) {
    colinfo <- DBI::dbGetQuery(connV, 
                               paste0("SELECT ",
                                      "COLUMN_NAME AS 'col', ",
                                      "DATA_TYPE AS 'type', ",
                                      "CHARACTER_MAXIMUM_LENGTH AS 'length'",
                                      "FROM INFORMATION_SCHEMA.COLUMNS ",
                                      "WHERE TABLE_NAME = '", names(tables[t]), "'",
                                      "ORDER BY ORDINAL_POSITION"))
    diff <- inner_join(diff, colinfo)
    for(x in 1:nrow(diff)) {
      qa <- rbind(qa, data.frame(names(tables[t]), diff[x,]))
    }
  }
}

colnames(qa) <- c("table", "column", "type", "length")

if(nrow(qa) > 0) {
  msg <- c("<style> table, th, td { border: 1px solid black; padding: 0 10px; } </style>",
           "<p>New Columns Added! - ", format(Sys.time(), "%m/%d/%Y %X"), "</p>", 
           htmlTable(qa, rnames = F),
           "<p>Warnings:</p>",
           "<p>", warnings(), "</p>")
  
  email <- compose_email(
    body = md(msg)
  )
  
  email %>%
    smtp_send(
      to = "jwhitehurst@kingcounty.gov",
      from = "jwhitehurst@kingcounty.gov",
      subject = paste0("AUTOMATED: MEO VertiQ Tables Change! ", format(Sys.Date(), "%m/%d/%Y")),
      credentials = creds_key("outlook")
    )
}
rm(list = ls())
