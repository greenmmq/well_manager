#
# This is a Shiny Webapp for managing PDB deployment and sampling...
# ... on groundwater monitoring networks. 
#

library(DBI)
library(DT)
library(dplyr)
library(RSQLite)
library(shiny)
library(shinyauthr)
library(tidyr)


# connect to pre-built SQLite database
conn <- dbConnect(RSQLite::SQLite(), dbname = "pdb_manager.db")


### Shiny WebApp ###


## UI Script ##

ui <- fluidPage(
  # add login panel UI function
  shinyauthr::loginUI(id = "login"),
  
  # navigation bar
  uiOutput("navbar"),
  
  # add logout button UI
  div(class = "pull-right", shinyauthr::logoutUI(id = "logout"))
)

# Define server logic 
server <- function(input, output, server) {
  
  ## LOGIN SERVER ##
  
  # call login module supplying data frame, 
  # user and password cols and reactive trigger
  credentials <- shinyauthr::loginServer(
    id = "login",
    data = data.frame(dbGetQuery(conn, "SELECT * FROM users")),
    user_col = user,
    pwd_col = password,
    log_out = reactive(logout_init())
  )
  
  # call the logout module with reactive trigger to hide/show
  logout_init <- shinyauthr::logoutServer(
    id = "logout",
    active = reactive(credentials()$user_auth)
  )
  
  ## OBSERVERS ##
  
  # listen for query inputs set to "All" and change choices as necessary
  observeEvent(input$site != "All", {
    dbtrigger$depend()
    updateSelectInput(
      inputId = "location",
      choices = c(
        "All",
        unique(loc()$Location[loc()$Site == input$site]
        )
      )
    )
  })
  
  # change deploy PDB site/location dropdowns to only include wells without pdbs
  observeEvent(input$dpSite, {
    dbtrigger$depend()
    site.no.pdb = filter(no_pdb(), Site == input$dpSite)
    updateSelectInput(
      inputId = "dpLocation",
      choices = site.no.pdb$Location
    )
  })
  
  # change collect sample site/location dropdowns to only include wells with pdbs
  observeEvent(input$csSite, {
    dbtrigger$depend()
    site.has.pdb = filter(has_pdb(), Site == input$csSite)
    updateSelectInput(
      inputId = "csLocation",
      choices = site.has.pdb$Location
    )
  })
  
  # listen for Create Well button and then execute function with message
  observeEvent(input$cwCreate, {
    # execute db function and display modal output
    message <- createWell(input$cwSite, input$cwLocation)
    showModal(modalDialog(
      title = "Well Creation Message",
      message, 
      easyClose = TRUE
    ))
    dbtrigger$trigger()
  })
  
  # listen for Deploy PDB button and then execute function with message
  observeEvent(input$dpDeploy, {
    # execute db function and display modal output
    message <- deployPDB(
      input$dpSite, input$dpLocation, as.character(input$dpDate), input$dpDTW
    )
    showModal(modalDialog(
      title = "PDB Deployment Message",
      message, 
      easyClose = TRUE
    ))
    dbtrigger$trigger()
  })
  
  # listen for Collect Sample button and then execute function with message
  observeEvent(input$csCollect, {
    # execute db function and display modal output
    message <- collectPDB(
      input$csSite, input$csLocation, as.character(input$csDate), 
      input$csDTW, input$csDepth, input$csPDB, input$csNotes
    )
    showModal(modalDialog(
      title = "PDB Collection Message",
      message, 
      easyClose = TRUE
    ))
    dbtrigger$trigger()
  })
  
  ## REACTIVES ##
  
  loc <- reactive({
    dbtrigger$depend()
    # df for filtering dropdowns on site/location
    dbGetQuery(conn, "SELECT Site, Location FROM locations")
  })
  
  # reactive selection for dropdowns on site/locations with PDBs deployed
  has_pdb <- reactive({
    dbtrigger$depend() 
    dbGetQuery(
               conn,
               "
               -- gets only wells where Sample_Date is NULL AKA w/ PDB
               SELECT l.Site, l.Location
               FROM locations l
               LEFT JOIN pdbs p  
               ON p.loc_id = l.loc_id
               LEFT JOIN samples s
               ON p.pdb_id = s.pdb_id
               INNER JOIN (
                 -- gets max pdb_id for a given well
                 SELECT l.Site, l.Location,
                   MAX(p.pdb_id) AS 'pdb_id'
                 FROM locations l
                 LEFT JOIN pdbs p
                 ON p.loc_id = l.loc_id
                 LEFT JOIN samples s
                 ON p.pdb_id = s.pdb_id
                 GROUP BY l.Site, l.Location
               ) a
               ON p.pdb_id = a.pdb_id
               WHERE s.Sample_Date IS NULL
               "
    )
  })
  
  # reactive selection for dropdowns on site/locations without PDBs deployed
  no_pdb <- reactive ({
    dbtrigger$depend()
    dbGetQuery(conn,
               "
               -- gets only wells w/out PDB
               SELECT Site, Location
               FROM locations
               WHERE loc_id NOT IN (
                 -- gets only wells where Sample_Date is NULL AKA w/ PDB
                 SELECT l.loc_id
                 FROM locations l
                 LEFT JOIN pdbs p  
                 ON p.loc_id = l.loc_id
                 LEFT JOIN samples s
                 ON p.pdb_id = s.pdb_id
                 INNER JOIN (
                   -- gets max pdb_id for a given well
                   SELECT l.Site, l.Location,
                     MAX(p.pdb_id) AS 'pdb_id'
                   FROM locations l
                   LEFT JOIN pdbs p
                   ON p.loc_id = l.loc_id
                   LEFT JOIN samples s
                   ON p.pdb_id = s.pdb_id
                   GROUP BY l.Site, l.Location
                 ) a
                 ON p.pdb_id = a.pdb_id
                 WHERE s.Sample_Date IS NULL
               )
               "
    )
  })
  
  # reactive filtering for Query table
  query_table <- reactive ({
    dbtrigger$depend()
    # conditional logic to filter on inputs
    if (input$site == "All" & input$location == "All") {
      if (input$datetype == "Deployment Date") {
        table <- selectAllDeployedRange(
          after = as.character(input$daterange[1]), 
          before = as.character(input$daterange[2])
        )
      } else {
        table <- selectAllSampledRange(
          after = as.character(input$daterange[1]), 
          before = as.character(input$daterange[2])
        )
      }
    } else if (input$site != "All" & input$location == "All") {
      if (input$datetype == "Deployment Date") {
        table <- selectSiteDeployedRange(
          site = input$site,
          after = as.character(input$daterange[1]), 
          before = as.character(input$daterange[2])
        )
      } else {
        table <- selectSiteSampledRange(
          site = input$site,
          after = as.character(input$daterange[1]), 
          before = as.character(input$daterange[2])
        )
      } 
    } else {
      if (input$datetype == "Deployment Date") {
        table <- selectLocationDeployedRange(
          name = input$location,
          site = input$site,
          after = as.character(input$daterange[1]), 
          before = as.character(input$daterange[2])
        )
      } else {
        table <- selectLocationSampledRange(
          name = input$location,
          site = input$site,
          after = as.character(input$daterange[1]), 
          before = as.character(input$daterange[2])
        )
      }
    }
    # conditional logic to subset query by pdb status
    if (input$pdbstatus == 1) {
      table
    } else if (input$pdbstatus == 2) {
      pdbSubsetter(table, has_pdb())
    } else if (input$pdbstatus == 3) {
      pdbSubsetter(table, no_pdb())
    }
  })
  
  ## OUTPUTS ##
  
  # homepage UI
  output$homepage <- renderUI({
    req(credentials()$user_auth)
    tags$h4("Welcome to the PDB Manager! 
            This application is designed to help field teams track and manage PDBs in groundwater monitoring wells.")
    })
  
  # renders the Query table reactive results
  output$query <- DT::renderDT(
    query_table()
    # options = list(paging = TRUE,    ## paginate the output
    #                pageLength = 15,  ## number of rows to output for each page
    #                scrollX = TRUE,   ## enable scrolling on X axis
    #                scrollY = TRUE,   ## enable scrolling on Y axis
    #                autoWidth = TRUE, ## use smart column width handling
    #                server = FALSE,   ## use client-side processing
    #                dom = 'Bfrtip',
    #                buttons = c('csv', 'excel'),
    #                columnDefs = list(list(targets = '_all', className = 'dt-center'),
    #                                  list(targets = c(0, 8, 9), visible = FALSE))
    # )
    # extensions = 'Buttons'
    # selection = 'single' ## enable selection of a single row
    # filter = 'bottom'              ## include column filters at the bottom
    # rownames = FALSE                ## don't show row numbers/names
  )
  
  # renders the navbar, requiring the loginUI
  output$navbar <- renderUI({
    req(credentials()$user_auth)
    tagList(
      navbarPage(
        id = "tabs",
        title = "PDB-Manager",
        tabPanel(
          "Home", 
          loginUI("login"), 
          uiOutput("homepage")    
        ),
        # sidebar panel layout for the table queries
        tabPanel("Explore",
          sidebarLayout(
            sidebarPanel(
              tags$h3("Use this page to explore PDB deployment and sampling information."),
              selectInput(
                "site", 
                "Select Site:", 
                choices = c("All", unique(loc()$Site)),
                selected = "All"
              ),
              selectInput(
                "location", 
                "Select Location:", 
                choices = c("All", unique(loc()$Location)),
                selected = "All"
              ),
              radioButtons(
                "datetype", 
                "Date Range Filters on:",
                choices = c("Deployment Date", "Sample Date"),
                selected = "Deployment Date"
              ),
              dateRangeInput(
                "daterange",
                "Select Date Range:",
                start = "2010-01-01",
                end = "2023-01-01",
                min = "2000-01-01",
                max = "2040-01-01",
                format = "yyyy-mm-dd",
                separator = " to "
              ),
              radioButtons(
                "pdbstatus", 
                "Subeset selection by:",
                choiceNames = list(
                  "All Wells",
                  "Deployed PDBs Only", 
                  "Last Sample from Wells without a PDB Deployed"
                ),
                choiceValues = list(1, 2, 3)
              )
            ),
            # query table output
            mainPanel(
              DT::dataTableOutput("query")
            )
          )
        ),
        # Basic layout for the create well layout
        tabPanel("Create Well",
          tags$h3("Use this page to add new wells to the database."),
          textInput("cwSite", "Enter the Site name:"),
          textInput("cwLocation", "Enter the Well name:"),
          actionButton("cwCreate", "Create")
        ),
        # Basic layout for the deploy PDB layout
        tabPanel("Deploy PDB",
          tags$h3(
            "Use this page to record when a PDB is deployed to a well."
          ),
          tags$h5(
            "NOTE 1. Record the depths in units of feet below-top-of-casing (ft btoc)."
          ),
          tags$h5(
            "NOTE 2. Only available for selection are sites and wells without a PDB currently deployed."
          ),
          selectInput(
            "dpSite", 
            "Select Site:", 
            choices = unique(no_pdb()$Site)
          ),
          selectInput(
            "dpLocation", 
            "Select Well:", 
            choices = unique(no_pdb()$Location)
          ),
          dateInput(
            "dpDate", 
            "Select the date the PDB was deployed:"
          ),
          numericInput(
            "dpDTW", 
            "Enter the depth-to-water (ft btoc) at the time of deployment:",
            value=0
          ),
          actionButton("dpDeploy", "Deploy")
        ),
        # Basic layout for the collect sample layout
        tabPanel("Collect Sample",
          tags$h3(
            "Use this page to record when a PDB is collected from a well and sampled."
          ),
          tags$h5(
            "NOTE 1. Record the depths in units of feet below-top-of-casing (ft btoc)."
          ),
          tags$h5(
            "NOTE 2. Only available for selection are sites and wells with a PDB currently deployed."
          ),
          selectInput(
            "csSite", 
            "Select Site:", 
            choices = unique(has_pdb()$Site)
          ),
          selectInput(
            "csLocation", 
            "Select Well:", 
            choices = unique(has_pdb()$Location)
          ),
          dateInput(
            "csDate", 
            "Select the date the PDB was collected and sampled:"
          ),
          numericInput(
            "csDTW", 
            "Enter the depth-to-water (ft btoc) at the time of collection:",
            value=0
          ),
          numericInput(
            "csDepth", 
            "Enter the depth-to-sample (ft btoc) at the time of collection:",
            value=0
          ),
          radioButtons(
            "csPDB", 
            "Was a PDB Redeployed?",
            choices = c("Y", "N")
          ),
        textInput("csNotes", "Enter any notes about the sample or PDB:"),
        actionButton("csCollect", "Collect")
        ),
      )
    )
  })
}


### DATABASE CRU FUNCTIONS ###


# query to create a new well
createWell = function(site, name){
  # check if location already exists
  locid = dbGetQuery(conn,
                     "
                     SELECT MAX(loc_id) AS loc_id 
                     FROM locations
                     WHERE Site = ? AND Location = ?
                     ",
                     params=c(site, name)
  )$loc_id
  # if it exists then do nothing and give error message
  if (!is.na(locid)) {
    paste('ERROR! - Well:', name, 'already exists at Site:', site,'.')
  } else { # else add the new well into locations and success message
    dbExecute(conn,
              "INSERT INTO locations (Site, Location) VALUES (?, ?)",
              params=c(site, name)
              )
    paste('Well:', name, 'successfully added to Site:', site, '!')
  }
}

# query to deploy a PDB
deployPDB = function(site, name, date, dtw){
  # get the specified loc_id
  locid = dbGetQuery(conn,
                     "
                     SELECT loc_id FROM locations 
                     WHERE Site = ? AND Location = ?
                     ",
                     params=c(site, name)
  )$loc_id
  
  # determine if a pdb is deployed by getting the max pdb_id of...
  # ... any pdbs not yet sampled IE Sample_Date is null
  status = dbGetQuery(conn, 
                      "
                      SELECT 
                        MAX(p.pdb_id) AS 'status'
                      FROM pdbs AS p
                      INNER JOIN (
                        SELECT pdb_id, Sample_Date
                        FROM samples
                        WHERE Sample_Date IS NULL
                      ) AS s
                      ON p.pdb_id = s.pdb_id
                      WHERE loc_id = ?
                      ",
                      params=c(locid)
  )$status
  
  # if a pdb exists, error message and end function
  # indicate pdb_id and deployment date of existing well to user
  if (!is.na(status)) {
    existing = dbGetQuery(conn,
                          "
                          SELECT 
                            MAX(pdb_id) AS 'pdb',
                            Deployment_Date AS 'date'
                          FROM pdbs
                          WHERE loc_id = ?
                          ",
                          params=c(locid)
    )
    paste('ERRROR! - PDB ID:', existing$pdb,
          ' already deployed to well:', name, 
          ' on:', existing$date, '.'
    )
  } else { # insert deployment details into pdbs table and success message
    dbExecute(conn,
              "
                INSERT INTO pdbs (loc_id, Deployment_Date, Deployment_DTW_btoc)
                VALUES (?, DATE(?), ?)
                ",
              params=c(locid, date, dtw)
    )
    
    # find the new pdb_id
    max = dbGetQuery(conn, "SELECT MAX(pdb_id) AS 'max' FROM pdbs")$max
    
    # create a new sample associated to the pdb_id
    dbExecute(conn,
              "
                INSERT INTO samples (pdb_id)
                VALUES (?)
                ",
              params=c(max)
    )
    paste('PDB successfully deployed to well:', name, 
          ' on:', date, '!'
    )
  }
}

# query to collect a PDB
collectPDB = function(site, name, date, dtw, depth, pdb, notes) {
  # finds max pdb_id not yet sampled at the given location 
  pdbid = dbGetQuery(conn,
                     "
                     SELECT MAX(s.pdb_id) AS 'max'
                     FROM pdbs AS p
                       INNER JOIN (
                         SELECT loc_id
                         FROM locations
                         WHERE Site = ? AND Location = ?
                       ) AS l
                       ON p.loc_id = l.loc_id
                       INNER JOIN (
                         SELECT pdb_id
                         FROM samples
                         WHERE Sample_Date IS NULL
                       ) AS s
                       ON p.pdb_id = s.pdb_id
                     ",
                     params=c(site, name)
  )$max
  # pdbid is null IE no pdbid has a null Sample_Date then error out
  if (is.na(pdbid)) {
    paste('ERROR! - There is no PDB deployed in well:', name, '.')
  } else { # update the sample associated with the pdbid queried above
    dbExecute(conn, 
              "
                UPDATE samples
                SET 
                  Sample_Date = DATE(?), 
                  Sample_DTW_btoc = ?,
                  Sample_Depth_btoc = ?, 
                  New_PDB = ?,
                  Notes = ?
                WHERE pdb_id = ?;
                ",
              params=c(date, dtw, depth, pdb, notes, pdbid)
    )
    # redeploy a new pdb if indicated
    if (pdb == "Y") {
      deployPDB(site, name, date, dtw)
      paste('Sample successfully collected PDB in well:', name, 
            '!', 'A PDB successfully redeployed to the well!'
            )
    } else {
      paste('Sample successfully collected PDB in well:', name, 
            '!', 'A PDB was not redeployed to the well.'
      )
    }
  }
}

# query to view all data deployed between date range
selectAllDeployedRange = 
  function(after='2000-01-01', before='2040-01-01'){
    dbGetQuery(conn,
               "
               SELECT 
                 l.Site, l.Location, p.Deployment_Date, p.Deployment_DTW_btoc,
                 s.Sample_Date, s.Sample_DTW_btoc, s.Sample_Depth_btoc, 
                 s.New_PDB, s.Notes
               FROM pdbs AS p
                 INNER JOIN (
                   SELECT *
                   FROM locations
                 ) AS l
                 ON p.loc_id = l.loc_id
                 INNER JOIN (
                   SELECT *
                   FROM samples
                 ) AS s
                 ON p.pdb_id = s.pdb_id
               WHERE DATE(p.Deployment_Date) >= DATE(?) 
                 AND DATE(p.Deployment_Date) < DATE(?)
               ORDER BY DATE(s.Sample_Date) DESC, Site, Location
               ",
               params=c(after, before)
    )
  }

# query to view all data sampled between date range
selectAllSampledRange = 
  function(after='2000-01-01', before='2040-01-01'){
    dbGetQuery(conn,
               "
               SELECT 
                 l.Site, l.Location, p.Deployment_Date, p.Deployment_DTW_btoc,
                 s.Sample_Date, s.Sample_DTW_btoc, s.Sample_Depth_btoc, 
                 s.New_PDB, s.Notes
               FROM pdbs AS p
                 INNER JOIN (
                   SELECT *
                   FROM locations
                 ) AS l
                 ON p.loc_id = l.loc_id
                 INNER JOIN (
                   SELECT *
                   FROM samples
                 ) AS s
                 ON p.pdb_id = s.pdb_id
               WHERE DATE(s.Sample_Date) >= DATE(?) 
                 AND DATE(s.Sample_Date) < DATE(?)
               ORDER BY DATE(s.Sample_Date) DESC, Site, Location
               ",
               params=c(after, before)
    )
  }

# query to view all data from one site deployed between date range
selectSiteDeployedRange = 
  function(site, after='2000-01-01', before='2040-01-01'){
    dbGetQuery(conn,
               "
               SELECT 
                 l.Site, l.Location, p.Deployment_Date, p.Deployment_DTW_btoc,
                 s.Sample_Date, s.Sample_DTW_btoc, s.Sample_Depth_btoc, 
                 s.New_PDB, s.Notes
               FROM pdbs AS p
                 INNER JOIN (
                   SELECT *
                   FROM locations
                   WHERE Site = ?
                 ) AS l
                 ON p.loc_id = l.loc_id
                 INNER JOIN (
                   SELECT *
                   FROM samples
                 ) AS s
                 ON p.pdb_id = s.pdb_id
               WHERE DATE(p.Deployment_Date) >= DATE(?) 
                 AND DATE(p.Deployment_Date) < DATE(?)
               ORDER BY Site, Location, DATE(s.Sample_Date) DESC
               ",
               params=c(site, after, before)
    )
  }

# query to view all data from one site sampled between date range
selectSiteSampledRange = 
  function(site, after='2000-01-01', before='2040-01-01'){
    dbGetQuery(conn,
               "
               SELECT 
                 l.Site, l.Location, p.Deployment_Date, p.Deployment_DTW_btoc,
                 s.Sample_Date, s.Sample_DTW_btoc, s.Sample_Depth_btoc, 
                 s.New_PDB, s.Notes
               FROM pdbs AS p
                 INNER JOIN (
                   SELECT *
                   FROM locations
                   WHERE Site = ?
                 ) AS l
                 ON p.loc_id = l.loc_id
                 INNER JOIN (
                   SELECT *
                   FROM samples
                 ) AS s
                 ON p.pdb_id = s.pdb_id
               WHERE DATE(s.Sample_Date) >= DATE(?) 
                 AND DATE(s.Sample_Date) < DATE(?)
               ORDER BY Site, Location, DATE(s.Sample_Date) DESC
               ",
               params=c(site, after, before)
    )
  }

# query to view all data from one location deployed between date range
selectLocationDeployedRange = 
  function(site, name, after='2000-01-01', before='2040-01-01'){
    dbGetQuery(conn,
               "
               SELECT 
                 l.Site, l.Location, p.Deployment_Date, p.Deployment_DTW_btoc,
                 s.Sample_Date, s.Sample_DTW_btoc, s.Sample_Depth_btoc, 
                 s.New_PDB, s.Notes
               FROM pdbs AS p
                 INNER JOIN (
                   SELECT *
                   FROM locations
                   WHERE Site = ? AND Location = ?
                 ) AS l
                 ON p.loc_id = l.loc_id
                 INNER JOIN (
                   SELECT *
                   FROM samples
                 ) AS s
                 ON p.pdb_id = s.pdb_id
               WHERE DATE(p.Deployment_Date) >= DATE(?) 
                 AND DATE(p.Deployment_Date) < DATE(?)
               ORDER BY Site, Location, DATE(s.Sample_Date) DESC
               ",
               params=c(site, name, after, before)
    )
  }

# query to view all data from one location sampled between date range
selectLocationSampledRange = 
  function(site, name, after='2000-01-01', before='2040-01-01'){
    dbGetQuery(conn,
               "
               SELECT 
                 l.Site, l.Location, p.Deployment_Date, p.Deployment_DTW_btoc,
                 s.Sample_Date, s.Sample_DTW_btoc, s.Sample_Depth_btoc, 
                 s.New_PDB, s.Notes
               FROM pdbs AS p
                 INNER JOIN (
                   SELECT *
                   FROM locations
                   WHERE Site = ? AND Location = ?
                 ) AS l
                 ON p.loc_id = l.loc_id
                 INNER JOIN (
                   SELECT *
                   FROM samples
                 ) AS s
                 ON p.pdb_id = s.pdb_id
               WHERE DATE(s.Sample_Date) >= DATE(?) 
                 AND DATE(s.Sample_Date) < DATE(?)
               ORDER BY Site, Location, DATE(s.Sample_Date) DESC
               ",
               params=c(site, name, after, before)
    )
  }


### SUPPORTING FUNCTIONS ###


# function to subset the query table on wells w/ or w/o PDBs
# ... based on reactive_table input
pdbSubsetter = function(table, reactive_table) {
  # get the max deployment_date only on wells in reactive_table
  filter.table <- 
    inner_join(table, reactive_table, 
      by = c("Site"="Site", "Location"="Location")
    ) %>%
    group_by(Site, Location) %>%
    summarise(Deployment_Date= max(Deployment_Date)) %>%
    ungroup() 
  # rejoin the rest of the query data to remaining pdb deployments
  new.table <- 
    inner_join(table, filter.table,
      by = c(
        "Site"="Site", 
        "Location"="Location", 
        "Deployment_Date"="Deployment_Date" 
      )
    )
  return(new.table)
}

# reactive db query re-exeecution trigger
# https://www.r-bloggers.com/2020/08/shiny-in-production-app-and-database-syncing/
makereactivetrigger <- function() {
  rv <- reactiveValues(a = 0)
  list(
    depend = function() {
      rv$a
      invisible()
    },
    trigger = function() {
      rv$a <- isolate(rv$a + 1)
    }
  )
}
dbtrigger <- makereactivetrigger()


### Run the application ###

shinyApp(ui = ui, server = server)

