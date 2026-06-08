# app.R -------------------------------------------------------------
library(shiny)
library(dplyr)
library(tibble)
library(tidyr)
library(ggplot2)
library(lubridate)


source("OMOP_LogFileParseShiny.R")

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
          div(
            style = "max-height: 800px; overflow-y: auto; border: 1px solid #eee; padding: 6px;",
            uiOutput("etl_gantt_ui")
          ),
          tableOutput("etl_times")
        ),
        tabPanel(
          "Executed scripts for source data loading",
          tableOutput("executing_scripts")
        ),
        tabPanel(
          "Run and skipped scripts",
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
    
    # Build compact labels like: "maitt + ohdsi_cdm_202207"
    compact_names <- vapply(seq_along(res_list), function(i) {
      x <- res_list[[i]]
      
      # fallback if parse failed
      if (isTRUE(x$error) || is.null(x$summary) || !nrow(x$summary)) {
        return(input$logs$name[i])
      }
      
      td <- x$summary$TARGETDB[1]
      sc <- x$summary$TARGETDB_SCHEMA_OHDSI_CDM[1]
      
      if (is.na(td) || is.na(sc) || !nzchar(td) || !nzchar(sc)) {
        input$logs$name[i]
      } else {
        paste(td, sc, sep = " | ")
      }
    }, character(1))
    
    # Ensure uniqueness if multiple logs have same TARGETDB+SCHEMA
    make_unique <- function(x) {
      idx <- ave(seq_along(x), x, FUN = seq_along)
      cnt <- ave(seq_along(x), x, FUN = length)
      ifelse(cnt > 1 & idx > 1, paste0(x, " (", idx, ")"), x)
    }
    compact_names <- make_unique(compact_names)
    
    names(res_list) <- compact_names
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
      
      print(x$parameetrid)
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
    
    table_order <- c(
      "metadata",
      "care-site",
      "provider",
      "person",
      "visit-occurrence",
      "visit-detail",
      "condition-occurrence",
      "observation",
      "procedure-occurrence",
      "measurement",
      "drug-exposure",
      "device-exposure",
      "death",
      "cost",
      "observation-period",
      "condition-era",
      "drug-era",
      "cdm-source"
    )
    
    table_dir_map <- c(
      "metadata"             = "sqlscripts/19-metadata",
      "care-site"            = "sqlscripts/4-care-site",
      "provider"             = "sqlscripts/5-provider",
      "person"               = "sqlscripts/6-person",
      "visit-occurrence"     = "sqlscripts/7-visit-occurrence",
      "visit-detail"         = "sqlscripts/7-1-visit-detail",
      "condition-occurrence" = "sqlscripts/8-condition-occurrence",
      "observation"          = "sqlscripts/9-observation",
      "procedure-occurrence" = "sqlscripts/10-procedure-occurrence",
      "measurement"          = "sqlscripts/11-measurement",
      "drug-exposure"        = "sqlscripts/12-drug-exposure",
      "device-exposure"      = "sqlscripts/13-device-exposure",
      "death"                = "sqlscripts/14-death",
      "cost"                 = "sqlscripts/20-cost",
      "observation-period"   = "sqlscripts/15-observation-period",
      "condition-era"        = "sqlscripts/16-condition-era",
      "drug-era"             = "sqlscripts/17-drug-era",
      "cdm-source"           = "sqlscripts/18-cdm-source"
    )
    
    # Long format from parsed logs
    long_list <- lapply(file_names, function(fname) {
      x <- res_list[[fname]]
      if (isTRUE(x$error) || is.null(x$etl_times) || !nrow(x$etl_times)) return(NULL)
      
      x$etl_times %>%
        transmute(
          table = .data$table,
          File  = fname,
          value = .data$duration_hms
        ) %>%
        filter(.data$table %in% table_order)
    })
    long <- bind_safe(long_list)
    
    # Wide table
    wide_times <- if (!is.null(long) && nrow(long)) {
      long$File <- factor(long$File, levels = file_names)
      
      long %>%
        mutate(table = factor(.data$table, levels = table_order)) %>%
        select(table, File, value) %>%
        tidyr::pivot_wider(
          names_from  = File,
          values_from = value,
          names_sort  = FALSE
        )
    } else {
      tibble(table = factor(character(0), levels = table_order))
    }
    
    # Base rows
    base <- tibble(
      Step      = seq_along(table_order),
      table     = factor(table_order, levels = table_order),
    )
    
    out <- base %>%
      left_join(wide_times, by = "table") %>%
      arrange(Step) %>%
      rename(`ETL table` = table)
    
    out
  })
  
  # ------------------------------------------------------------------
  # Gantt plot
  # ------------------------------------------------------------------
  output$etl_gantt_ui <- renderUI({
    res_list <- parsed_store()
    req(length(res_list))

    table_order <- c(
      "metadata","care-site","provider","person","visit-occurrence","visit-detail",
      "condition-occurrence","observation","procedure-occurrence","measurement",
      "drug-exposure","device-exposure","death","cost","observation-period",
      "condition-era","drug-era","cdm-source"
    )
    
    n_files  <- length(res_list)
    n_tables <- length(table_order)
    
    px_per_table <- 12  # vertical space per ETL table label
    px_per_facet <- 70    # extra space per logfile
    
    height_px <- max(700, n_files * (n_tables * px_per_table + px_per_facet))
    
    plotOutput("etl_gantt", height = paste0(height_px, "px"))
  })
  output$etl_gantt <- renderPlot({
    res_list <- parsed_store()
    req(length(res_list))
    
    file_names <- names(res_list)
    
    table_order <- c(
      "metadata",
      "care-site",
      "provider",
      "person",
      "visit-occurrence",
      "visit-detail",
      "condition-occurrence",
      "observation",
      "procedure-occurrence",
      "measurement",
      "drug-exposure",
      "device-exposure",
      "death",
      "cost",
      "observation-period",
      "condition-era",
      "drug-era",
      "cdm-source"
    )
    
    # HH:MM:SS -> seconds
    hms_to_sec <- function(x) {
      x <- as.character(x)
      out <- rep(NA_real_, length(x))
      ok <- !is.na(x) & grepl("^\\d{2}:\\d{2}:\\d{2}$", x)
      if (any(ok)) {
        parts <- strsplit(x[ok], ":", fixed = TRUE)
        h <- as.numeric(vapply(parts, `[`, character(1), 1))
        m <- as.numeric(vapply(parts, `[`, character(1), 2))
        s <- as.numeric(vapply(parts, `[`, character(1), 3))
        out[ok] <- h * 3600 + m * 60 + s
      }
      out
    }
    
    sec_to_hms <- function(sec) {
      sec <- pmax(0, round(sec))
      h <- sec %/% 3600
      m <- (sec %% 3600) %/% 60
      s <- sec %% 60
      sprintf("%02d:%02d:%02d", h, m, s)
    }
    
    # Build runtime data from parsed logs
    long_list <- lapply(file_names, function(fname) {
      x <- res_list[[fname]]
      if (isTRUE(x$error) || is.null(x$etl_times) || !nrow(x$etl_times)) return(NULL)
      
      x$etl_times %>%
        transmute(
          table = .data$table,
          File  = fname,
          duration_hms = .data$duration_hms
        ) %>%
        filter(.data$table %in% table_order)
    })
    
    long <- bind_safe(long_list)
    validate(need(!is.null(long) && nrow(long), "No ETL runtimes found to plot."))
    
    df <- long %>%
      mutate(
        table = factor(table, levels = table_order),
        duration_sec = hms_to_sec(duration_hms),
        duration0 = ifelse(is.na(duration_sec), 0, duration_sec)
      ) %>%
      arrange(File, table) %>%
      group_by(File) %>%
      mutate(
        start_sec = lag(cumsum(duration0), default = 0),
        end_sec   = start_sec + duration_sec
      ) %>%
      ungroup() %>%
      filter(!is.na(duration_sec))

    df$table <- factor(df$table, levels = rev(table_order))
    
    ggplot(df, aes(y = table)) +
      geom_segment(aes(x = start_sec, xend = end_sec, yend = table), linewidth = 6, lineend = "butt") +
      facet_wrap(~File, ncol = 1) +
      scale_x_continuous(
        name = "Time since ETL start (HH:MM:SS)",
        labels = function(x) sec_to_hms(x),
        expand = expansion(mult = c(0, 0.01))
      ) +
      ylab("ETL table") +
      theme_minimal(base_size = 16) +
      theme(
        panel.grid.major.y = element_blank(),
        strip.text = element_text(face = "bold", size = 16),
        axis.text.y = element_text(size = 14),
        panel.spacing.y = grid::unit(1.2, "lines")
      )
  })
  
  
  # ------------------------------------------------------------------
  # Executing scripts: wide table
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
      mutate(value = script_name) %>%
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
  # Run and skipped scripts table
  # ------------------------------------------------------------------
  output$condition_occ <- renderTable({
    res_list <- parsed_store()
    req(length(res_list))
    
    file_names <- names(res_list)
    
    table_order <- c(
      "care-site",
      "cdm-source",
      "condition-era",
      "condition-occurrence",
      "cost",
      "death",
      "device-exposure",
      "drug-era",
      "drug-exposure",
      "measurement",
      "metadata",
      "observation",
      "observation-period",
      "person",
      "procedure-occurrence",
      "provider",
      "visit-detail",
      "visit-occurrence"
    )
    
    extract_table_from_exec_path <- function(script_path) {
      dir_raw <- sub("^sqlscripts/([^/]+)/.*$", "\\1", script_path)
      sub("^(?:\\d+-)+", "", dir_raw, perl = TRUE)
    }
    
    normalize_script_name <- function(script_file) {
      script_file |>
        stringr::str_remove("^([A-Z]\\d+|\\d+)-") |>
        stringr::str_remove("-to-staging\\.sql$") |>
        stringr::str_remove("\\.sql$")
    }
    
    parse_source_detail <- function(table, script_name) {
      if (is.na(table) || is.na(script_name)) {
        return(list(source = NA_character_, detail = NA_character_))
      }
      
      prefix <- paste0(table, "-")
      if (!startsWith(script_name, prefix)) {
        return(list(source = NA_character_, detail = NA_character_))
      }
      
      rest  <- substr(script_name, nchar(prefix) + 1, nchar(script_name))
      parts <- strsplit(rest, "-", fixed = TRUE)[[1]]
      
      source <- NA_character_
      remain <- character(0)
      
      if (length(parts) >= 2 && parts[1] %in% c("cancer", "death") && parts[2] == "registry") {
        source <- paste(parts[1:2], collapse = "-")   
        remain <- parts[-c(1, 2)]
      } else if (length(parts) >= 3 && parts[1] == "cda" && parts[2] == "fact" && parts[3] == "extraction") {
        source <- "cda-fact-extraction"
        remain <- parts[-c(1, 2, 3)]
      } else {
        source <- parts[1]
        remain <- parts[-1]
      }
      
      detail <- if (length(remain) > 0) paste(remain, collapse = "-") else "(default/no subtype)"
      
      list(source = source, detail = detail)
    }
    
   
    long_list <- lapply(file_names, function(fname) {
      x <- res_list[[fname]]
      if (isTRUE(x$error)) return(NULL)
      
      exec_out <- NULL
      if (!is.null(x$executing_scripts) && nrow(x$executing_scripts)) {
        exec_tbl <- x$executing_scripts
      
        if ("is_B" %in% names(exec_tbl)) exec_tbl <- dplyr::filter(exec_tbl, .data$is_B)
        
        if (nrow(exec_tbl)) {
          tbl <- extract_table_from_exec_path(exec_tbl$script_path)
          parsed <- Map(parse_source_detail, tbl, exec_tbl$script_name)
          
          exec_out <- tibble(
            table  = tbl,
            source = vapply(parsed, `[[`, character(1), "source"),
            detail = vapply(parsed, `[[`, character(1), "detail"),
            File   = fname,
            status = "run"
          )
        }
      }
      
      skip_out <- NULL
      if (!is.null(x$skipped) && nrow(x$skipped)) {
        skip_tbl <- x$skipped |>
          dplyr::filter(stringr::str_detect(.data$file, "^B\\d{1,3}-"))
        
        if (nrow(skip_tbl)) {
          tbl <- skip_tbl$directory
          sn  <- normalize_script_name(skip_tbl$file)
          parsed <- Map(parse_source_detail, tbl, sn)
          
          skip_out <- tibble(
            table  = tbl,
            source = vapply(parsed, `[[`, character(1), "source"),
            detail = vapply(parsed, `[[`, character(1), "detail"),
            File   = fname,
            status = "skipped"
          )
        }
      }
      
      out <- dplyr::bind_rows(exec_out, skip_out)
      if (!nrow(out)) return(NULL)
      
      out |>
        dplyr::filter(!is.na(.data$table), !is.na(.data$source), !is.na(.data$detail)) |>
        dplyr::filter(.data$table %in% table_order)
    })
    
    long <- bind_safe(long_list)
    
    
    if (is.null(long) || !nrow(long)) {
      return(tibble(info = "No run/skipped B-scripts found."))
    }
    
    long <- long |>
      dplyr::group_by(table, source, detail, File) |>
      dplyr::summarise(
        status = if (any(status == "run")) "run" else "skipped",
        .groups = "drop"
      ) |>
      dplyr::mutate(
        is_default = detail == "(default/no subtype)",
        display = dplyr::case_when(
          status == "run"     & is_default ~ sprintf('<span style="color: lightgreen; font-weight: bold;">%s</span>', detail),
          status == "run"                  ~ sprintf('<span style="color: green; font-weight: bold;">%s</span>', detail),
          status == "skipped" & is_default ~ sprintf('<span style="color: lightcoral; font-weight: bold;">%s</span>', detail),
          TRUE                             ~ sprintf('<span style="color: red; font-weight: bold;">%s</span>', detail)
        )
      ) %>%
      dplyr::select(-is_default)
    
    cells <- long |>
      dplyr::group_by(table, source, File) |>
      dplyr::summarise(
        value = paste(display, collapse = ", "),
        .groups = "drop"
      )
    
    wide <- cells |>
      dplyr::mutate(
        File  = factor(File, levels = file_names),
        table = factor(table, levels = table_order)
      ) |>
      tidyr::pivot_wider(
        names_from  = File,
        values_from = value,
        names_sort  = FALSE
      ) |>
      dplyr::arrange(table, source) |>
      dplyr::rename(
        `ETL table` = table,
        Source       = source
      )

    missing_tables <- setdiff(table_order, as.character(unique(wide$`ETL table`)))
    if (length(missing_tables)) {
      blank <- tibble(
        `ETL table` = factor(missing_tables, levels = table_order),
        Source       = '<span style="font-style: italic;">(no Source found)</span>'
      )
      for (fn in file_names) blank[[fn]] <- NA_character_
      
      wide <- dplyr::bind_rows(wide, blank) |>
        dplyr::arrange(`ETL table`, Source)
    }
    
    wide
  }, sanitize.text.function = function(x) x)
  # ------------------------------------------------------------------
  # Skipped scripts
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
      group_by(directory, reason, LogFile) %>%
      summarise(
        Scripts = paste(sort(unique(file)), collapse = "<br>"),
        .groups = "drop"
      ) %>%
      arrange(directory, reason, LogFile) %>%
      rename(
        Directory = directory,
        Reason    = reason
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
