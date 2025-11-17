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
          "Executing scripts",
          tableOutput("executing_scripts")
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
          File        = fname,
          is_B        = .data$is_B
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
        value = ifelse(
          is_B,
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
    
    long %>%
      arrange(directory, file, LogFile) %>%
      rename(
        Directory = directory,
        Reason    = reason,
        Script    = file
      )
  })
  
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
