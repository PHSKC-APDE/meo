library(taskscheduleR)

# https://cran.r-project.org/web/packages/taskscheduleR/readme/README.html

taskscheduler_create(taskname = "meo_vertiq.qa", 
                     rscript = "C:/temp/Rscripts/vertiq.qa.R", 
                     schedule = "DAILY", 
                     startdate = format(Sys.Date(), "%m/%d/%Y"), 
                     starttime = "07:00")

taskscheduler_create(taskname = "meo.covid.vac", 
                     rscript = "C:/temp/Rscripts/meo.covid.vac.R", 
                     schedule = "DAILY", 
                     startdate = format(Sys.Date(), "%m/%d/%Y"), 
                     starttime = "09:00")

taskscheduler_delete("meo_vertiq.qa")

taskscheduler_ls()
