#### Pulls queries for Julia and saves them to Excel files on the network
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

### Call Libraries

suppressWarnings(library(dplyr))
suppressWarnings(library(odbc)) # Read to and write from SQL
suppressWarnings(library(keyring)) # Access stored credentials
suppressWarnings(library(glue)) # Safely combine code and variables
suppressWarnings(library(xlsx)) # Read and write xlsx Excel Files

message("--------------------------------------------------")
message(paste0(Sys.time(), " - Begin Script"))

### Set Database Connection

conn <- DBI::dbConnect(odbc::odbc(),
                       driver = "ODBC Driver 17 for SQL Server",
                       server = "tcp:cme2.database.windows.net",
                       database = "KingProd",
                       uid = keyring::key_list("meo_vertiq")[["username"]],
                       pwd = keyring::key_get("meo_vertiq", keyring::key_list("meo_vertiq")[["username"]]),
                       Encrypt = "yes")

### Pull data with queries

results_cases <- DBI::dbGetQuery(
  conn,
  glue::glue_sql("
    SELECT 
	    C.CaseNum AS 'CaseNum',
	    D.DeathDate AS 'DeathDate',
	    D.BirthDate AS 'BirthDate',
	    D.AgeYears AS 'AgeYears',
	    GN.Description AS 'Gender',
		  DI.Ethnicity AS 'Ethnicity',
	    ND.LastName AS 'NameLast',
	    ND.FirstName AS 'NameFirst',
	    N.NarrSceneDescription AS 'CaseNarrative',
	    DI.Race AS 'Race',
	    A.Addr AS 'Address',
	    A.City AS 'City',
	    A.County AS 'County',
	    A.stte AS 'State',
	    A.ZipCode AS 'ZipCode'
    FROM Cases C
	    LEFT JOIN Decedents D ON D.Id = C.DecedentId
	    LEFT JOIN NamesData ND ON C.IdentificationAttemptId = ND.Id
	    LEFT JOIN Narratives N ON N.Id = C.NarrativeId
	    LEFT JOIN Items GN ON GN.Id = D.GenderId
	    LEFT JOIN DecedentInformation DI ON DI.CaseNum = C.CaseNum
	    LEFT JOIN Addresses A ON D.AddressId = A.Id
    WHERE 
	    C.CaseNum LIKE '18%' 
	    OR C.CaseNum LIKE '19%' 
	    OR C.CaseNum LIKE '20%' 
	    OR C.CaseNum LIKE '21%' 
    ORDER BY 
	    C.CaseNum",
                 .con = conn))

results_incident <- DBI::dbGetQuery(
  conn,
  glue::glue_sql("
    SELECT 
    	C.CaseNum AS 'CaseNum',
	    HS.Description AS 'HousingStatus',
	    I.IncidentPlace AS 'EventPlaceType',
	    A.Addr AS 'EventAddr',
	    A.City AS 'EventCity',
	    A.ZipCode AS 'EventZip',
	    A.County AS 'EventCounty'
    FROM Cases C
	    LEFT JOIN Decedents D ON C.DecedentId = D.Id
	    LEFT JOIN Items HS ON HS.Id = D.HomelessId
	    LEFT JOIN Incidents I ON I.Id = C.IncidentId
	    LEFT JOIN Addresses A ON I.AddressId = A.Id
    WHERE 
	    C.CaseNum LIKE '18%' 
	    OR C.CaseNum LIKE '19%' 
	    OR C.CaseNum LIKE '20%' 
	    OR C.CaseNum LIKE '21%' 
    ORDER BY 
	    C.CaseNum",
                 .con = conn))

sub_dc <- DBI::dbGetQuery(
  conn,
  glue::glue_sql("
    SELECT 
	    C.CaseNum AS 'CaseNum',
	    PR.Description AS 'PendingReason',
	    HR.Description AS 'HeroinRelated',
	    M.Description AS 'Manner',
	    SM.Description AS 'SubManner',
	    D.Id AS 'DCId',
	    D.DrugRelated AS 'Probable OD',
	    D.OtherCause AS 'CauseOther',
	    D.HowInjury AS 'InjuryDescription',
		NP.CompleteName AS 'AutopsyPathologist',
		NPA.CompleteName AS 'AutopsyPathologistAsst',
		ND.CompleteName AS 'DeathCertSignedBy',
		PCD.nme AS 'DeathCertSignedByCat'
	  FROM Cases C
	    LEFT JOIN DeathCertificates D ON D.Id = C.DeathCertificateId
	    LEFT JOIN NamesData ND ON D.SignedById = ND.Id
	    LEFT JOIN People PD ON ND.Id = PD.NameDataId
	    LEFT JOIN PersonCategories PCD ON PD.PersonCategoryId = PCD.Id
		LEFT JOIN Autopsies A ON C.AutopsyId = A.Id
		LEFT JOIN NamesData NP ON A.PathologistId = NP.Id
	    LEFT JOIN NamesData NPA ON A.PathologistAssistantId = NPA.Id
	    LEFT JOIN Items PR ON PR.Id = D.CauseOfHoldId
	    LEFT JOIN Items HR ON HR.Id = D.CauseHeroinRelatedId
	    LEFT JOIN Items M ON M.Id = C.MannerOfDeathId
	    LEFT JOIN Items SM ON SM.Id = C.SubMannerOfDeathId
    WHERE 
	    C.CaseNum LIKE '18%' 
	    OR C.CaseNum LIKE '19%' 
	    OR C.CaseNum LIKE '20%' 
	    OR C.CaseNum LIKE '21%' 
    ORDER BY 
	    C.CaseNum",
                 .con = conn))

sub_cod <- DBI::dbGetQuery(
  conn,
  glue::glue_sql("
    SELECT 
		Z.DeathCertificateId AS 'DCId',
		Z.Id AS 'CodId',
		Z.DeathCause
    FROM Cases C
		INNER JOIN DeathCertificateCauses Z ON C.DeathCertificateId = Z.DeathCertificateId
    WHERE 
	    C.CaseNum LIKE '18%' 
	    OR C.CaseNum LIKE '19%' 
	    OR C.CaseNum LIKE '20%' 
	    OR C.CaseNum LIKE '21%' 
    ORDER BY 
	    C.CaseNum, Z.DeathCertificateId, Z.Id",
                 .con = conn))

results_location <- DBI::dbGetQuery(
  conn,
  glue::glue_sql("
    SELECT 
	    C.CaseNum AS 'CaseNum',
	    DL.DeathPlace AS 'DeathPlace',
	    O.nme AS 'Hospital',
	    A.Addr AS 'Address',
	    A.City AS 'City',
	    A.ZipCode AS 'Zip',
	    A.County AS 'County',
	    A.stte AS 'State'
    FROM Cases C
	    LEFT JOIN DeathLocations DL ON C.DeathLocationId = DL.Id
	    LEFT JOIN Addresses A ON DL.DeathAddressId = A.Id
	    LEFT JOIN Organizations O ON DL.DeathPlaceOrganizationId = O.Id
    WHERE 
	    C.CaseNum LIKE '18%' 
	    OR C.CaseNum LIKE '19%' 
	    OR C.CaseNum LIKE '20%' 
	    OR C.CaseNum LIKE '21%' 
    ORDER BY 
	    C.CaseNum",
                 .con = conn))

results_tox <- DBI::dbGetQuery(
  conn,
  glue::glue_sql("
    SELECT 
	    C.CaseNum AS 'CaseNum',
	    T.EnteredDate AS 'ToxResultsEnteredDate',
	    T.Test AS 'Test',
	    T.Analyte AS 'Analyte',
	    T.Result AS 'ScreenResult',
	    IIF(ISNUMERIC(T.Result) = 1, CAST(Result AS FLOAT), NULL) AS 'QuantResultsResult',
	    Specimen AS 'ScreenResultsSpecimen',
	    ISNULL(T.QualitativeResult, IIF(ISNUMERIC(T.Result) = 1, NULL, T.Result)) AS 'QualitativeResult',
	    ISNULL(Q.Description, T.Range) AS 'QuantResultRange',
	    T.UnitsOfMeasure AS 'QuantResultUnit'
    FROM Cases C
	    INNER JOIN ToxResults T ON C.Id = T.CaseId
	    LEFT JOIN Items Q ON T.QuantResultRangeId = Q.Id
    WHERE 
	    (C.CaseNum LIKE '18%' 
	    OR C.CaseNum LIKE '19%' 
	    OR C.CaseNum LIKE '20%') 
	    AND T.Deleted = 0
    ORDER BY 
		    C.CaseNum, T.Analyte", 
                 .con = conn))

message("Pulling Data from SQL Complete")

### Determine Causes A-D and combine sub queries into one data frame
df <- data.frame(matrix(ncol = 5, nrow = 0))
colnames(df) <- c("DCId", "CauseA", "CauseB", "CauseC", "CauseD")
z = 1

for (x in 1:nrow(sub_cod)) {
  if (x == 1) { 
    df[z,1] <- sub_cod[x,1] 
  } else if (is.na(df[z,1]) == T) { 
    df[z,1] <- sub_cod[x,1] 
  } else if ((df[z,1] == sub_cod[x,1]) == !T) { 
    z = z + 1
    df[z,1] <- sub_cod[x,1]
  }
  if (is.na(df[z,2]) == T) {
    df[z,2] <- sub_cod[x, 3]
  } else if (is.na(df[z,3]) == T) {
    df[z,3] <- sub_cod[x, 3]
  } else if (is.na(df[z,4]) == T) {
    df[z,4] <- sub_cod[x, 3]
  } else {
    df[z,5] <- sub_cod[x, 3]
    z = z + 1
  }
}

results_deathcert <- left_join(sub_dc, df)

### Reorganize dataframe columns due to odbc restrictions
results_cases <- results_cases[,c(1,7,8,9,2,3,4,5,10,6,11,12,13,14,15)]
results_deathcert <- results_deathcert[,-6]
results_deathcert <- results_deathcert[,c(1,13,14,15,16,7,2,8,3,4,5,6,9,10,11,12)]

message(" Reorganizing Data Complete")

### Output data to Excel Files

temp_dir <- glue::glue("C:/temp/meo")
save_dir <- glue::glue("//Phdata01/yesl_data/OPIOID PROJECTS/MEO Data")

file.remove(list.files(temp_dir, include.dirs = F, full.names = T, recursive = T))

write.xlsx(results_cases,
           glue::glue("{temp_dir}/Julia_Cases.xlsx"),
           row.names = F,
           showNA = F)
write.xlsx(results_incident,
           glue::glue("{temp_dir}/Julia_Incident.xlsx"),
           row.names = F,
           showNA = F)
write.xlsx(results_deathcert,
           glue::glue("{temp_dir}/Julia_DeathCertificate.xlsx"),
           row.names = F,
           showNA = F)
write.xlsx(results_location,
           glue::glue("{temp_dir}/Julia_Location.xlsx"),
           row.names = F,
           showNA = F)
write.xlsx(results_tox,
           glue::glue("{temp_dir}/Julia_ToxResults.xlsx"),
           row.names = F,
           showNA = F)

file.copy(list.files(temp_dir, include.dirs = F, full.names = T, recursive = T), save_dir, overwrite = T)
file.remove(list.files(temp_dir, include.dirs = F, full.names = T, recursive = T))

message("Writing Data to Excel Complete")

### Clear out global environment

rm(list = ls())

message(paste0(Sys.time(), " - Script Complete"))
