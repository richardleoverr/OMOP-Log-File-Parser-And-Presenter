# app.R -------------------------------------------------------------
library(shiny)
library(dplyr)
library(tibble)
library(tidyr)

# This should define parse_log()
source("OMOP_LogFileParseShiney.R")

ui <- fluidPage(
  titlePanel("ETL Log Parser"),
  sidebarLayout(
    sidebarPanel(
      fileInput(
        "logs", "Upload log files",
        multiple = TRUE
      ),
      uiOutput("fileList"),
      hr(),
      helpText("Upload multiple logs. Tables show all files side-by-side where possible.")
    ),
    mainPanel(
      tabsetPanel(
        tabPanel(
          "Summary",
          h4("Summary / Parameters / Metadata / Sources"),
          tableOutput("summary_all")
        ),
        tabPanel(
          "ETL runtimes (per table)",
          tableOutput("etl_times")
        ),
        tabPanel(
          "Executing scripts for B",
          tableOutput("executing_scripts")
        ),
        tabPanel(
          "Condition occurrence",
          tableOutput("condition_occ")
        ),
        tabPanel(
          "Skipped scripts",
          tableOutput("skipped")
        ),
        tabPanel(
          "Errors",
          tableOutput("errors")
        )
      )
    )
  )
)

server <- function(input, output, session) {
  # Helper: safely bind non-null tibbles
  bind_safe <- function(lst) {
    lst <- Filter(Negate(is.null), lst)
    if (!length(lst)) return(NULL)
    bind_rows(lst)
  }
  
  # Stores a named list: names = file names, values = parse_log(...) results
  parsed_store <- reactiveVal(list())
  
  observeEvent(input$logs, {
    req(input$logs)
    files <- input$logs
    
    res_list <- lapply(seq_len(nrow(files)), function(i) {
      path <- files$datapath[i]
      safe_res <- try(parse_log(path), silent = TRUE)
      if (inherits(safe_res, "try-error")) {
        list(error = TRUE, msg = as.character(safe_res))
      } else {
        safe_res
      }
    })
    
    names(res_list) <- input$logs$name
    parsed_store(res_list)
  }, ignoreInit = TRUE)
  
  # Show list of loaded files in the sidebar
  output$fileList <- renderUI({
    files <- names(parsed_store())
    if (!length(files)) return(NULL)
    tags$div(
      h5("Loaded files:"),
      tags$ul(lapply(files, tags$li))
    )
  })
  
  # ------------------------------------------------------------------
  # SUMMARY TAB: one big wide table
  # ------------------------------------------------------------------
  output$summary_all <- renderTable({
    res_list <- parsed_store()
    req(length(res_list))
    
    file_names <- names(res_list)
    
    # ---------- SUMMARY ----------
    summary_long <- bind_safe(lapply(file_names, function(fname) {
      x <- res_list[[fname]]
      
      if (isTRUE(x$error)) {
        tibble(
          Section = "Summary",
          field   = "Message",
          File    = fname,
          value   = x$msg
        )
      } else {
        x$summary %>%
          mutate(
            Section = "Summary",
            File    = fname
          ) %>%
          tidyr::pivot_longer(
            cols      = -c(Section, File),
            names_to  = "field",
            values_to = "value"
          )
      }
    }))
    
    # ---------- PARAMETERS ----------
    params_long <- bind_safe(lapply(file_names, function(fname) {
      x <- res_list[[fname]]
      if (isTRUE(x$error) || is.null(x$parameetrid)) return(NULL)
      
      x$parameetrid %>%
        mutate(
          Section = "Parameters",
          File    = fname
        ) %>%
        tidyr::pivot_longer(
          cols      = -c(Section, File),
          names_to  = "field",
          values_to = "value"
        )
    }))
    
    # ---------- METADATA ----------
    meta_long <- bind_safe(lapply(file_names, function(fname) {
      x <- res_list[[fname]]
      if (isTRUE(x$error) || is.null(x$metaandmed)) return(NULL)
      
      x$metaandmed %>%
        mutate(
          Section = "Metadata",
          File    = fname
        ) %>%
        tidyr::pivot_longer(
          cols      = -c(Section, File),
          names_to  = "field",
          values_to = "value"
        )
    }))
    
    # ---------- SOURCES ----------
    sources_long <- bind_safe(lapply(file_names, function(fname) {
      x <- res_list[[fname]]
      if (isTRUE(x$error) || is.null(x$allikad)) return(NULL)
      
      x$allikad %>%
        mutate(
          Section = "Sources",
          File    = fname
        ) %>%
        tidyr::pivot_longer(
          cols      = -c(Section, File),
          names_to  = "field",
          values_to = "value"
        )
    }))
    
    # Combine all sections
    long <- bind_safe(list(summary_long, params_long, meta_long, sources_long))
    req(!is.null(long))
    
    long$File <- factor(long$File, levels = file_names)
    
    wide <- long %>%
      select(Section, field, File, value) %>%
      tidyr::pivot_wider(
        names_from  = File,
        values_from = value,
        names_sort  = FALSE
      ) %>%
      arrange(
        factor(Section, levels = c("Summary", "Parameters", "Metadata", "Sources")),
        field
      ) %>%
      rename(
        Block     = Section,
        Parameter = field
      )
    
    wide
  })
  
  # ------------------------------------------------------------------
  # ETL runtimes: wide table
  # ------------------------------------------------------------------
  output$etl_times <- renderTable({
    res_list <- parsed_store()
    req(length(res_list))
    
    file_names <- names(res_list)
    
    long_list <- lapply(file_names, function(fname) {
      x <- res_list[[fname]]
      if (isTRUE(x$error) || is.null(x$etl_times) || !nrow(x$etl_times)) return(NULL)
      
      x$etl_times %>%
        transmute(
          table = .data$table,
          File  = fname,
          value = .data$duration_hms
        )
    })
    
    long <- bind_safe(long_list)
    if (is.null(long)) {
      return(tibble(info = "No ETL runtimes found."))
    }
    
    long$File <- factor(long$File, levels = file_names)
    
    wide <- long %>%
      select(table, File, value) %>%
      tidyr::pivot_wider(
        names_from  = File,
        values_from = value,
        names_sort  = FALSE
      ) %>%
      arrange(table) %>%
      rename(Table = table)
    
    wide
  })
  
  # ------------------------------------------------------------------
  # Executing scripts: wide table
  #   rows   = "slots" (no visible label column)
  #   cols   = file names
  #   values = script name (green & bold if B-script)
  # ------------------------------------------------------------------
  output$executing_scripts <- renderTable({
    res_list <- parsed_store()
    req(length(res_list))
    
    file_names <- names(res_list)
    
    long_list <- lapply(file_names, function(fname) {
      x  <- res_list[[fname]]
      df <- x$executing_scripts
      if (isTRUE(x$error) || is.null(df) || !nrow(df)) return(NULL)
      
      df %>%
        transmute(
          script_name = .data$script_name,
          File        = fname
        ) %>%
        distinct(script_name, File, .keep_all = TRUE)
    })
    
    long <- bind_safe(long_list)
    
    if (is.null(long)) {
      return(tibble(info = "No Executing script lines found."))
    }
    
    long$File <- factor(long$File, levels = file_names)
    
    wide <- long %>%
      mutate(
        highlight = stringr::str_detect(script_name, "^condition-occurrence-"),
        value = ifelse(
          highlight,
          sprintf('<span style="color: green; font-weight: bold;">%s</span>', script_name),
          script_name
        )
      ) %>%
      select(script_name, File, value) %>%
      tidyr::pivot_wider(
        names_from  = File,
        values_from = value,
        names_sort  = FALSE
      ) %>%
      arrange(script_name) %>%
      select(-script_name)
    
    wide
  }, sanitize.text.function = function(x) x)
  
  # ------------------------------------------------------------------
  # Condition occurrence tab:
  #   rows   = table name ("condition_occurrence") + source
  #   cols   = log file names
  #   cells  = details (diagnoses/services/ncsp/dispensing...) with
  #            green = executed, red = skipped
  # ------------------------------------------------------------------
  output$condition_occ <- renderTable({
    res_list <- parsed_store()
    req(length(res_list))
    
    file_names <- names(res_list)
    
    
    parse_condition_name <- function(file_or_name) {
      base <- sub(".*/", "", file_or_name)
      base <- gsub("^([A-Z][0-9]+|[0-9]+)-", "", base)
      base <- gsub("-to-staging\\.sql$", "", base)
      base <- gsub("\\.sql$", "", base)

      if (!grepl("^condition-occurrence-", base)) {
        return(list(source = NA_character_, detail = NA_character_))
      }
      
      rest  <- sub("^condition-occurrence-", "", base)
      parts <- strsplit(rest, "-", fixed = TRUE)[[1]]
      if (length(parts) < 2) {
        return(list(source = parts[1], detail = NA_character_))
      }
      source <- parts[1]
      detail <- paste(parts[-1], collapse = "-")
      list(source = source, detail = detail)
    }
    
    long_list <- lapply(file_names, function(fname) {
      x <- res_list[[fname]]
      
      exec <- x$executing_scripts
      exec_co <- NULL
      if (!isTRUE(x$error) && !is.null(exec) && nrow(exec)) {
        masks <- grepl("condition-occurrence-", exec$script_file)
        if (any(masks)) {
          sub_df <- exec[masks, , drop = FALSE]
          parsed <- lapply(sub_df$script_file, parse_condition_name)
          src    <- vapply(parsed, function(z) z$source, character(1))
          det    <- vapply(parsed, function(z) z$detail, character(1))
          
          exec_co <- tibble(
            table  = "condition_occurrence",
            source = src,
            detail = det,
            File   = fname,
            status = "run"
          ) %>%
            filter(!is.na(source), !is.na(detail))
        }
      }
      
      skip <- x$skipped
      skip_co <- NULL
      if (!isTRUE(x$error) && !is.null(skip) && nrow(skip)) {
        masks <- grepl("condition-occurrence-", skip$file)
        if (any(masks)) {
          sub_df <- skip[masks, , drop = FALSE]
          parsed <- lapply(sub_df$file, parse_condition_name)
          src    <- vapply(parsed, function(z) z$source, character(1))
          det    <- vapply(parsed, function(z) z$detail, character(1))
          
          skip_co <- tibble(
            table  = "condition_occurrence",
            source = src,
            detail = det,
            File   = fname,
            status = "skipped"
          ) %>%
            filter(!is.na(source), !is.na(detail))
        }
      }
      
      bind_rows(exec_co, skip_co)
    })
    
    long <- bind_safe(long_list)
    
    if (is.null(long) || !nrow(long)) {
      return(tibble(info = "No condition-occurrence scripts found."))
    }
    
    long <- long %>%
      group_by(table, source, detail, File) %>%
      summarise(
        status = if (any(status == "run")) "run" else "skipped",
        .groups = "drop"
      )
    
    long <- long %>%
      mutate(
        display = ifelse(
          status == "run",
          sprintf('<span style="color: green; font-weight: bold;">%s</span>', detail),
          sprintf('<span style="color: red; font-weight: bold;">%s</span>', detail)
        )
      )

    cells <- long %>%
      group_by(table, source, File) %>%
      summarise(
        value = paste(display, collapse = ", "),
        .groups = "drop"
      )

    wide <- cells %>%
      mutate(File = factor(File, levels = file_names)) %>%
      tidyr::pivot_wider(
        names_from  = File,
        values_from = value,
        names_sort  = FALSE
      ) %>%
      arrange(source) %>%
      rename(
        `Table name` = table,
        Source       = source
      )
    
    wide
  }, sanitize.text.function = function(x) x)
  # ------------------------------------------------------------------
  # Skipped scripts: long table with file column
  # ------------------------------------------------------------------
  output$skipped <- renderTable({
    res_list <- parsed_store()
    req(length(res_list))
    
    file_names <- names(res_list)
    
    long_list <- lapply(file_names, function(fname) {
      x  <- res_list[[fname]]
      df <- x$skipped
      if (isTRUE(x$error) || is.null(df) || !nrow(df)) return(NULL)
      
      df %>%
        mutate(LogFile = fname) %>% 
        select(directory, reason, file, LogFile)
    })
    
    long <- bind_safe(long_list)
    
    if (is.null(long)) {
      return(tibble(info = "No skipped scripts found."))
    }
    
    table <- long %>%
      arrange(directory, file, LogFile) %>%
      rename(
        Directory = directory,
        Reason    = reason,
        Script    = file
      ) %>%
      mutate(
        Script = ifelse(
          stringr::str_detect(Script, "condition-occurrence"),
          sprintf('<span style="color: red; font-weight: bold;">%s</span>', Script),
          Script
        )
      )
    
    table
  }, sanitize.text.function = function(x) x)
  
  # ------------------------------------------------------------------
  # Errors: wide table without label column
  # ------------------------------------------------------------------
  output$errors <- renderTable({
    res_list <- parsed_store()
    req(length(res_list))
    
    file_names <- names(res_list)
    
    long_list <- lapply(file_names, function(fname) {
      x <- res_list[[fname]]
      df <- x$errors
      if (isTRUE(x$error) || is.null(df) || !nrow(df)) return(NULL)
      
      df %>%
        transmute(
          error = .data$error,
          File  = fname,
          value = .data$error
        ) %>%
        distinct(error, File, .keep_all = TRUE)
    })
    
    long <- bind_safe(long_list)
    
    if (is.null(long)) {
      return(tibble(info = "No ERROR lines found."))
    }
    
    long$File <- factor(long$File, levels = file_names)
    
    wide <- long %>%
      select(error, File, value) %>%
      tidyr::pivot_wider(
        names_from  = File,
        values_from = value,
        names_sort  = FALSE
      ) %>%
      arrange(error) %>%
      select(-error)
    
    wide
  })
}

shinyApp(ui, server)
