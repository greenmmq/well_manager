#
# This is a Shiny Webapp for managing PDB deployment and sampling...
# ... on groundwater monitoring networks. 
#

library(shiny)
library(shinyauthr)
library(RSQLite)
library(DBI)
library(tidyr)
library(dplyr)
library(ggplot2)

# connect to pre-built SQLite database
conn <- dbConnect(RSQLite::SQLite(), dbname = "pdb_manager.db")

### Shiny WebApp ###

loc <- dbGetQuery(conn, "SELECT * FROM locations")

ui <- fluidPage(
  # add login panel UI function
  shinyauthr::loginUI(id = "login"),
  
  # navigation bar
  uiOutput("navbar"),
  
  # add logout button UI
  div(class = "pull-right", shinyauthr::logoutUI(id = "logout"))
)

# Define server logic required to draw a histogram
server <- function(input, output, server) {
  
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
  
  observeEvent(input$site != "All", {
    updateSelectInput(inputId = "location",
                      choices = c("All",
                                  unique(loc$Location[loc$Site == input$site])
                                  )
                      )
  })
  
  query_table <- reactive ({
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
  })
  
  output$homepage <- renderUI({
    req(credentials()$user_auth)
    tags$h1("Something in the homepage")
  })
  
  output$query <- renderTable({
    req(credentials()$user_auth)
    query_table()
  })
  
  output$navbar <- renderUI({
    req(credentials()$user_auth)
    tagList(
      navbarPage(
        id = "tabs",
        title = "PDB-Manager",
        tabPanel("Home", 
                 loginUI("login"), 
                 uiOutput("homepage")
        ),
        tabPanel("Queries",
                 sidebarLayout(
                   sidebarPanel(
                     selectInput("site", 
                                 "Select Site:", 
                                 choices = c("All", unique(loc$Site)),
                                 selected = "All"
                     ),
                     selectInput("location", 
                                 "Select Location:", 
                                 choices = c("All", unique(loc$Location)),
                                 selected = "All"
                     ),
                     radioButtons("datetype", 
                                  "Date Range Filters on:",
                                  choices = c("Deployment Date", "Sample Date"),
                                  selected = "Deployment Date"
                     ),
                     dateRangeInput("daterange",
                                    "Select Date Range:",
                                    start = "2010-01-01",
                                    end = "2022-01-01",
                                    min = "2000-01-01",
                                    max = "2040-01-01",
                                    format = "yyyy-mm-dd",
                                    separator = " to "
                     )
                   ),
                   
                   mainPanel(
                     tableOutput("query")
                   )
                 )
        )
      )
    )
  })
}



### Database Function List ###

# query to create a new well
createWell = function(site, location){
  dbExecute(conn,
            "INSERT INTO locations (Site, Location) VALUES (?, ?)",
            params=c(site, location)
  )
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
    paste('Error! - PDB ID:', existing$pdb,
          ' already deployed to well:', name, 
          ' on:', existing$date, '.'
    )
  } else { # insert deployment details into pdbs table
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
  }
}

# query to collect a PDB
collectPDB = function(site, name, date, dtw, depth, pdb, notes){
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
    paste('Error! - There is no PDB deployed in well:', name, '.')
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
selectSiteSampledRange = function(site, after='2000-01-01', before='2040-01-01'){
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



### Run the application ###
shinyApp(ui = ui, server = server)
