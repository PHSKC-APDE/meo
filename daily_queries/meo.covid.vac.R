#### Pulls queries for Julia and saves them to Excel files on the network
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

### Call Libraries
suppressWarnings(library(tidyverse))
suppressWarnings(library(odbc)) # Read to and write from SQL
suppressWarnings(library(keyring)) # Access stored credentials
suppressWarnings(library(glue)) # Safely combine code and variables
suppressWarnings(library(xlsx)) # Read and write xlsx Excel Files
suppressWarnings(library(configr))
suppressWarnings(library(blastula))

config <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/GitHub/meo/config/vertiq.config.yaml")
email_list <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/GitHub/meo/config/email.config.yaml")

### Set Database Connection
conn <- DBI::dbConnect(odbc::odbc(),
                       driver = "ODBC Driver 17 for SQL Server",
                       server = "tcp:cme2.database.windows.net",
                       database = "KingProd",
                       uid = keyring::key_list("meo_vertiq")[["username"]],
                       pwd = keyring::key_get("meo_vertiq", keyring::key_list("meo_vertiq")[["username"]]),
                       Encrypt = "yes")

### Pull data with queries
results <- DBI::dbGetQuery(
  conn,
  glue::glue_sql("
                 SELECT 
                  COALESCE (C.CaseNum, C.CCaseNum) AS CaseNum, 
                  ND.LastName, 
                  ND.FirstName, 
                  ND.MiddleName, 
                  D.AgeYears, 
                  D.BirthDate, 
                  D.DeathDate, 
                  GN.[Description] AS Gender, 
                  RC.[Description] AS Race, 
                  EN.[Description] AS Ethnicity, 
                  DCC.DeathCause, 
                  N.Synopsis, 
                  N.NarrativeDet
                 FROM Cases C
                  LEFT JOIN DeathCertificates DC 
                   ON DC.Id = C.DeathCertificateId
                  LEFT JOIN DeathCertificateCauses DCC 
                   ON DCC.DeathCertificateId = C.DeathCertificateId
                  LEFT JOIN Decedents D 
                   ON D.Id = C.DecedentId
                  LEFT JOIN NamesData ND 
                   ON ND.Id = C.IdentificationAttemptId
                  LEFT JOIN Decedents_Items DI 
                   ON DI.Id = D.Id
                  LEFT JOIN Items RC 
                   ON RC.Id = DI.Id2
                  LEFT JOIN Items EN 
                   ON EN.Id = D.EthnicityId
                  LEFT JOIN Items GN 
                   ON GN.ID = D.GenderId
                  LEFT JOIN Narratives N 
                   ON N.Id = C.NarrativeId
                 WHERE DC.CauseHoldOther LIKE '%cov%' 
                  AND DC.CauseHoldOther LIKE '%vac%'
                 ORDER BY COALESCE (C.CaseNum, C.CCaseNum)",
                 .con = conn))

### Output data to Excel Files
temp_dir <- "C:/temp/meo"
save_dir <- "\\\\phshare01\\cdi_share\\Analytics and Informatics Team\\COVID\\Data collection\\Vaccine Adverse Events\\VAE follow-up\\MEO Investigations"

### Get list of previous files and find most recent
file_list <- as.data.frame(list.files(save_dir))
if (nrow(file_list) > 0) {
  colnames(file_list) <- c("file_name")
  file_list$date_str <- substr(file_list$file_name, 21, 28)
  file_list$file_date <- as.Date(file_list$date_str, format = "%d%m%Y")
  prev_file <- file_list[which.max(file_list$file_date),1]
  prev_results <- read.xlsx(glue::glue("{save_dir}/{prev_file}"), sheetIndex = 1)

  ### Find new rows
  new_rows <- select(subset(results, !(CaseNum %in% prev_results$CaseNum)), CaseNum)
  if (nrow(new_rows) > 0) {
    new_rows$New <- 1
    results <- results %>% left_join(new_rows)
  }
}

file.remove(list.files(temp_dir, include.dirs = F, full.names = T, recursive = T))

### Create file and copy to network folder
file_name <- format(Sys.Date(), "%d%m%Y")
write.xlsx(results,
           glue::glue("{temp_dir}/RecentCOVIDVaccines_{file_name}.xlsx"),
           row.names = F,
           showNA = F)

file.copy(list.files(temp_dir, include.dirs = F, full.names = T, recursive = T), save_dir, overwrite = T)
file.remove(list.files(temp_dir, include.dirs = F, full.names = T, recursive = T))

### Send email with results
msg <- c("<p>", format(Sys.time(), "%m/%d/%Y %X"), " - New Recent COVID Vaccine Adverse Events File</p>",
         glue::glue("<p>File Name: <a href='{save_dir}/RecentCOVIDVaccines_{file_name}.xlsx'>
                    RecentCOVIDVaccines_{file_name}.xlsx</a></p>"),
         glue::glue("<p>Folder: <a href='{save_dir}'>{save_dir}</a></p>"),
         glue::glue("<p>New Cases: {nrow(new_rows)}</p>"))
  
email <- compose_email(
  body = md(msg))
  
email %>%
  smtp_send(
    to = email_list$emails_covid_vac,
    from = email_list$email_from,
    subject = paste0("AUTOMATED: MEO Recent COVID Vaccine Investigations ", format(Sys.Date(), "%m/%d/%Y")),
    credentials = creds_key("outlook")
  )

### Clear out global environment
rm(list = ls())

