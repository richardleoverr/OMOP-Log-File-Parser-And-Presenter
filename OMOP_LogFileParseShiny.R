# parser.R
library(stringr)
library(tibble)
library(lubridate)
library(dplyr)

parse_log <- function(log_path) {
  lines <- readLines(log_path, warn = FALSE)
  
  
  targetdb <- str_match(lines, "^TARGETDB:\\s*(.*)$")[,2]
  targetdb <- targetdb[!is.na(targetdb)][1]
  
  schema <- str_match(lines, "^TARGETDB_SCHEMA_OHDSI_CDM:\\s*(.*)$")[,2]
  schema <- schema[!is.na(schema)][1]
  
  regex_for_run_date <- "^[A-Z][a-z]{2} [A-Z][a-z]{2} \\s*\\d{1,2} \\d{2}:\\d{2}:\\d{2} [A-Z]{3,4} \\d{4}$"
  run_date <- lines[str_detect(lines, regex_for_run_date)][1]
  
  psql_line <- lines[str_detect(lines, "^psql\\b")][1]
  
  classifications <- str_match(psql_line,"--set=classifications=(\\S+)")[,2]
  bills <- str_match(psql_line,"--set=bills=(\\S+)")[,2]
  prescriptions <- str_match(psql_line,"--set=prescriptions=(\\S+)")[,2]
  hwisc_epi <- str_match(psql_line,"--set=hwisc_epi=(\\S+)")[,2]
  cda_fe <- str_match(psql_line,"--set=cda_fe=(\\S+)")[,2]
  patient_dem <- str_match(psql_line,"--set=patient_dem=(\\S+)")[,2]
  death_reg <- str_match(psql_line,"--set=death_reg=(\\S+)")[,2]
  vaccine_data <- str_match(psql_line,"--set=vaccine_data=(\\S+)")[,2]
  cancer_registry <- str_match(psql_line,"--set=cancer_registry=(\\S+)")[,2]
  pgs_calculations <- str_match(psql_line,"--set=pgs_calculations=(\\S+)")[,2]
  
  measurement_time_limit <- str_match(psql_line,"--set=measurement_time_limit=(\\S+)")[,2] |> gsub("^'|'$", "", x = _)
  death_year_limit <- str_match(psql_line,"--set=death_year_limit=(\\S+)")[,2]
  observation_time_start <- str_match(psql_line,"--set=observation_time_start=(\\S+)")[,2] |> gsub("^'|'$", "", x = _)
  observation_time_end <- str_match(psql_line,"--set=observation_time_end=(\\S+)")[,2] |> gsub("^'|'$", "", x = _)
  
  cdm_source_name <- str_match(psql_line,"--set=cdm_source_name=(\\S+)")[,2] |> gsub("^'|'$", "", x = _)
  cdm_source_abbreviation <- str_match(psql_line,"--set=cdm_source_abbreviation=(\\S+)")[,2] |> gsub("^'|'$", "", x = _)
  cdm_holder  <- str_match(psql_line,"--set=cdm_holder=(\\S+)")[,2] |> gsub("^'|'$", "", x = _)
  source_release_date  <- str_match(psql_line,"--set=source_release_date=(\\S+)")[,2] |> gsub("^'|'$", "", x = _)
  cdm_release_date  <- str_match(psql_line,"--set=cdm_release_date=(\\S+)")[,2] |> gsub("^'|'$", "", x = _)
  cdm_version  <- str_match(psql_line,"--set=cdm_version=(\\S+)")[,2] |> gsub("^'|'$", "", x = _)
  
  ajad <- str_extract(lines, "^[A-Z][a-z]*\\s[A-Z][a-z]*\\s{1,2}\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}\\s[A-Z]*\\s\\d{4}") |> na.omit()
  alguse_aeg <- ajad[1]
  lopp_aeg <- ajad[2]
  
  alguse_aeg_puhastus <- sub(" [A-Z]{3,4} ", " ", alguse_aeg)
  lopp_aeg_puhastus <- sub(" [A-Z]{3,4} ", " ", lopp_aeg)
  
  alguse_aeg <- as.POSIXct(alguse_aeg_puhastus, format = "%a %b %e %H:%M:%S %Y", tz = "Europe/Tallinn")
  lopp_aeg <- as.POSIXct(lopp_aeg_puhastus, format = "%a %b %e %H:%M:%S %Y", tz = "Europe/Tallinn")
  
  sec_to_hms <- function(x) {
    h <- x %/% 3600; m <- (x %% 3600) %/% 60; s <- x %% 60
    sprintf("%02d:%02d:%02d", h, m, s)
  }
  kulunud_aeg <- difftime(lopp_aeg, alguse_aeg, units = "sec") |> as.numeric() |> sec_to_hms()
  
  ETL_tabeli_nimed <- regmatches(
    lines, regexpr("Executing scripts from directory \\[sqlscripts/[^]]+\\]", lines, perl = TRUE)
  )
  ETL_tabeli_nimed <- sub(".*\\[sqlscripts/([^]/]+)\\].*", "\\1", ETL_tabeli_nimed)
  
  ETL_tabeli_nimed <- sub("^(?:\\d+-)+", "", ETL_tabeli_nimed, perl = TRUE)
  
  ETL_tabeli_ajad_indeksid <- grep("^Executing scripts from directory \\[sqlscripts/", lines) + 2L
  ETL_tabeli_ajad_indeksid <- ETL_tabeli_ajad_indeksid[ETL_tabeli_ajad_indeksid <= length(lines)]
  
  ETL_tabeli_ajad_read <- lines[ETL_tabeli_ajad_indeksid]
  ETL_tabeli_ajad_read <- regmatches(
    ETL_tabeli_ajad_read,
    regexpr("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}", ETL_tabeli_ajad_read, perl = TRUE)
  )
  
  lopp_aeg_short <- sub(" [A-Za-z]+$", "", lopp_aeg)
  ETL_tabeli_ajad_read <- append(ETL_tabeli_ajad_read, lopp_aeg_short)
  ETL_tabeli_ajad_read <- as.POSIXct(ETL_tabeli_ajad_read, format = "%Y-%m-%d %H:%M:%S", tz = "Europe/Tallinn")
  
  ETL_tabeli_kulunud_aeg <- as.numeric(diff(ETL_tabeli_ajad_read), units = "secs") |> sec_to_hms()
  ETL_map <- setNames(ETL_tabeli_kulunud_aeg, ETL_tabeli_nimed)
  
  skip_lines <- lines[str_detect(lines, "^Skipping file:")]
  skip_lines <- str_match(
    skip_lines,
    "^Skipping file:\\s+sqlscripts/([^/]+)/([^ ]+\\.sql)\\s+\\(([^)]+)\\)"
  )
  
  skipped_df <- tibble(
    directory = skip_lines[, 2],
    reason    = skip_lines[, 4],
    file      = skip_lines[, 3]
  ) |>
    filter(!is.na(file), !is.na(reason)) |>
    distinct() |>
    mutate(
      directory = sub("^(?:\\d+-)+", "", directory, perl = TRUE)
    )

  
  

  exec_lines <- lines[str_detect(lines, "Executing script:")]

  script_path <- str_match(
    exec_lines,
    "Executing script:\\s*\\[(sqlscripts/[^]]+)\\]"
  )[, 2]

  valid_idx <- !is.na(script_path)
  exec_lines   <- exec_lines[valid_idx]
  script_path  <- script_path[valid_idx]

  script_file <- sub(".*/", "", script_path)

  script_name <- script_file |>
    str_remove("^([A-Z]\\d+|\\d+)-") |>
    str_remove("-to-staging\\.sql$") |>
    str_remove("\\.sql$")
  
  is_B <- str_detect(script_file, "^B\\d{1,2}-")
  
  executing_scripts_tbl <- tibble(
    raw_line    = exec_lines,
    script_path = script_path,
    script_file = script_file,
    script_name = script_name,
    is_B        = is_B
  ) %>%
    filter(is_B)  
  
  
  

  error_lines <- lines[str_detect(lines,"ERROR:" )]
  error_loetelu <- sub('.*ERROR:\\s*', '', error_lines)
  
  allikad <- tibble(
    classifications, bills, prescriptions, hwisc_epi, cda_fe,
    patient_dem, death_reg, vaccine_data, cancer_registry, pgs_calculations
  )
  parameetrid <- tibble(
    measurement_time_limit, death_year_limit, observation_time_start, observation_time_end
  )
  metaandmed <- tibble(
    cdm_source_name, cdm_source_abbreviation, cdm_holder,
    source_release_date, cdm_release_date, cdm_version
  )
  
  result <- tibble(
    TARGETDB = targetdb,
    TARGETDB_SCHEMA_OHDSI_CDM = schema,
    run_date = run_date,
    elapsed_time = kulunud_aeg
  )

  etl_times_tbl <- tibble(table = names(ETL_map), duration_hms = unname(ETL_map))
  errors_tbl    <- tibble(error = error_loetelu)
  
  list(
    summary      = result,
    allikad      = allikad,
    parameetrid  = parameetrid,
    metaandmed   = metaandmed,
    etl_times    = etl_times_tbl,
    skipped      = skipped_df,
    errors       = errors_tbl,
    executing_scripts  = executing_scripts_tbl
  )
}
#result <- parse_log("C:/Users/Mina/Desktop/Lõputöö/Logifailid/etl_log_3_create_and_import_omop_cdm_2023_test_12-08-2025")

#print(result)

#print(result$etl_times[1,1])