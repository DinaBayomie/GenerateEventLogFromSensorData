library(readr)

library(foreach)


require (doParallel)
require(compiler)
registerDoParallel(makeCluster(4))

### data reading and preparation 
# E114_sample_dataset <- read_csv("C:/Dina/Project AI/UC1_Farplas/UC1_Farplas/OLD/E114_sample_dataset.csv")
# 
# # order by timestamp (date)
# ds<- E114_sample_dataset[order(E114_sample_dataset$date),]  
# ds <- ds[,-c(1)] ## to remove this unlabeled numberical column





### event  and case classes
setClass("event",slots = list(cId = "numeric", name = "character",  pvalue = "character", nvalue="character", date = "POSIXt" ))
setClass("case",slots=list(id="numeric", events = "list"))

setMethod("show","event",function(object) {
                         cat("EventName:",object@name, " : ")
                        cat("CaseId:",object@cId, " : ")
                         cat("pValue:", object@pvalue, " : ")
                        cat("nValue:", object@nvalue, " : ")
                        
                     })


### create case (as a list of events that share the same cid to be flatted csv ) based on the parameter that changed over two rows 
###such that row1 is the previous status and row2 is the new status that will form the case object
createCasePerRow <- function(row1,row2, cols_,cId,dateCol) {
  #  print("enter with ")
  #  print(row1)
   # print(row2)
    events_=list()
    #print(cols_)
    for (i in cols_) {
      pv = as.character(row1[1,i])
      nv = as.character(row2[1,i])
      
      if(pv != nv)
      {
        events_<-append(events_,new(Class = "event",cId = cId, name=paste("Change in"," ",i),
                                    pvalue = pv, nvalue=nv,date = as.POSIXlt.POSIXct(row2[1,dateCol])))
      }
    }
    #if(nrow(events_)>0)
    #{    
   # print("generated events ", length(events_))
       # return(new("case",id = cId,events=events_))
      return(events_)
  #  }
    #else
     # {return(events_)}
}


### create cases as list of events attached with case identifier form the sensor numerical stream data by observing marking the change over the entries as events
createCases <- function(dataset,dateCol) {
 
  colNames_ = colnames(dataset)
  colNames_ <-
    colNames_[lapply(colNames_, function(x)
      length(grep(dateCol, x, value = FALSE))) == 0]
  cId_ = 1
  events = list()
  for (i in 2:nrow(dataset)){
    c = createCasePerRow(row1 = dataset[i-1,], row2 = dataset[i,],cols_ = colNames_,cId = cId_ , dateCol = dateCol )  
    #print("returned events ", length(c))
    # if(!is.na(c)){
    if(length(c) > 0){    
    events<-append(events,c)
      cId_ = cId_+1
    }
    }
    return(events)
}



### [dataframe] create cases as list of events attached with case identifier form the sensor numerical stream data by observing marking the change over the entries as events
createCases <- function(dataset,dateCol,cid) {
  colNames_ = colnames(dataset)
  colNames_ <-
    colNames_[lapply(colNames_, function(x)
      length(grep(dateCol, x, value = FALSE))) == 0]
  cId_ = 1
  events <- data.frame(cId = numeric(), name = character(), 
                       pvalue = character(), nvalue=character(), date = character(),stringsAsFactors = FALSE)
  for (i in 2:nrow(dataset)){
       row1 = dataset[i-1,]
       row2 = dataset[i,]
       for (j in colNames_) {
         pv = as.character(row1[1,j])
         nv = as.character(row2[1,j])
         
         if(pv != nv)
         {
           e <- data.frame(cId = cid, name=paste("Change in"," ",j),
                                pvalue = pv, nvalue=nv,date =row2[1,dateCol],stringsAsFactors = FALSE)
           events <- rbind(events, e)
           
         }
       }
       cid=cid+1
     }
  return(events)
}

cmpCreateCases<- cmpfun(createCases)

##### split log by date  
splitDSday<-split(ds, as.Date(ds$date))
createAcasePerday<- function(dataset,cId,dateCol) {
  colNames_ = colnames(dataset)
  colNames_ <-
    colNames_[lapply(colNames_, function(x)
      length(grep(dateCol, x, value = FALSE))) == 0]
  cId_ = 1
  events <- data.frame(cId = numeric(), name = character(), 
                       pvalue = character(), nvalue=character(), date = character(),stringsAsFactors = FALSE)
  for (i in 2:nrow(dataset)){
    row1 = dataset[i-1,]
    row2 = dataset[i,]
    for (j in colNames_) {
      pv = as.character(row1[1,j])
      nv = as.character(row2[1,j])
      
      if(pv != nv)
      {
        e <- data.frame(cId = cId, name=paste("Change in"," ",j),
                        pvalue = pv, nvalue=nv,date =row2[1,dateCol],stringsAsFactors = FALSE)
        events <- rbind(events, e)
      }
    }
  }
  return(events)
}

createCasesPerday<-function(dataset,dateCol){
 # splitDSday<-split(dataset, as.Date(dataset[,dateCol]))
  cmpfunCreateAcasePerday<-cmpfun(createAcasePerday)
  casesPerDay <-foreach(t = iter(dataset, by = 'row'), cid= 1:length(splitDSday), .combine = rbind) %dopar% {
        cmpfunCreateAcasePerday(t,cid,dateCol)
    }
  return(casesPerDay)
}



### [dataframe] create cases as list of events attached with case identifier form 
###the sensor numerical stream data by observing marking the change over the entries as events#
### consider the status of the change and also consider change if its greater that delataInterval interval 
### for now its one delta for all sensors

createCasesRowDelta <- function(dataset,dateCol,cid,delta) {
  colNames_ = colnames(dataset)
  colNames_ <-
    colNames_[lapply(colNames_, function(x)
      length(grep(dateCol, x, value = FALSE))) == 0]
  cId_ = 1
  events <- data.frame(cId = numeric(), name = character(), 
                       pvalue = character(), nvalue=character(), status = character(), date = character(),stringsAsFactors = FALSE)
  for (i in 2:nrow(dataset)){
    row1 = dataset[i-1,]
    row2 = dataset[i,]
    for (j in colNames_) {
      pv = as.character(row1[1,j])
      nv = as.character(row2[1,j])
      if(j == "reason"){
        e <- data.frame(cId = cid, name=paste("Change in"," ",j), pvalue = pv, nvalue=nv,status="", date =row2[1,dateCol],stringsAsFactors = FALSE)
        events <- rbind(events, e)
      }
      else{
        if(pv != nv)
        {
          # print(j)
          # print(as.numeric(nv))
          # print(as.numeric(pv))
          if(is.na(as.numeric(nv))||is.na(as.numeric(pv))){
            e <- data.frame(cId = cid, name=paste("Change in"," ",j), pvalue = pv, nvalue=nv,status="", date =row2[1,dateCol],stringsAsFactors = FALSE)
            events <- rbind(events, e)
          }
          else{
            if(abs(as.numeric(nv)-as.numeric(pv))>delta){
              change = "+"
              if((as.numeric(nv)-as.numeric(pv))<0){
                change = "-"
              }
              e <- data.frame(cId = cid, name=paste("Change in"," ",j),
                              pvalue = pv, nvalue=nv,status=change, date =row2[1,dateCol],stringsAsFactors = FALSE)
              events <- rbind(events, e)
            }
            }
          }
      }
    }
    cid=cid+1
  }
  return(events)
}

createCasesRowDeltaUnifySensorType <- function(dataset,dateCol,cid,delta) {
  colNames_ = colnames(dataset)
  colNames_ <-
    colNames_[lapply(colNames_, function(x)
      length(grep(dateCol, x, value = FALSE))) == 0]
  cId_ = 1
  events <- data.frame(cId = numeric(), name = character(), 
                       pvalue = character(), nvalue=character(), status = character(), date = character(),stringsAsFactors = FALSE)
  for (i in 2:nrow(dataset)){
    row1 = dataset[i-1,]
    row2 = dataset[i,]
    for (j in colNames_) {
      pv = as.character(row1[1,j])
      nv = as.character(row2[1,j])
      # if(j == "reason"){
      #   e <- data.frame(cId = cid, name=paste("Change in"," ",gsub('[[:digit:]]+', '',j)), pvalue = pv, nvalue=nv,status="", date =row2[1,dateCol],stringsAsFactors = FALSE)
      #   events <- rbind(events, e)
      # }
      # else{
        if(pv != nv)
        {
          # print(j)
          # print(as.numeric(nv))
          # print(as.numeric(pv))
          if(is.na(as.numeric(nv))||is.na(as.numeric(pv))){
            e <- data.frame(cId = cid, name=paste("Change in"," ",gsub('[[:digit:]]+', '',j)), pvalue = pv, nvalue=nv,status="", date =row2[1,dateCol],stringsAsFactors = FALSE)
            events <- rbind(events, e)
          }
          else{
            if(abs(as.numeric(nv)-as.numeric(pv))>delta){
              change = "+"
              if((as.numeric(nv)-as.numeric(pv))<0){
                change = "-"
              }
              e <- data.frame(cId = cid, name=paste("Change in"," ",gsub('[[:digit:]]+', '',j)),
                              pvalue = pv, nvalue=nv,status=change, date =row2[1,dateCol],stringsAsFactors = FALSE)
              events <- rbind(events, e)
            }
          }
        }
      # }
    }
    cid=cid+1
  }
  return(events)
}



createCasesRowDeltaIncDecUnifySensorType <- function(dataset,dateCol,cid,delta) {
  colNames_ = colnames(dataset)
  colNames_ <-
    colNames_[lapply(colNames_, function(x)
      length(grep(dateCol, x, value = FALSE))) == 0]
  cId_ = 1
  events <- data.frame(cId = numeric(), name = character(), 
                       pvalue = character(), nvalue=character(), status = character(), date = character(),stringsAsFactors = FALSE)
  for (i in 2:nrow(dataset)){
    row1 = dataset[i-1,]
    row2 = dataset[i,]
    for (j in colNames_) {
      changeStr = "Change in"
      pv = as.character(row1[1,j])
      nv = as.character(row2[1,j])
      # if(j == "reason"){
      #   e <- data.frame(cId = cid, name=paste("Change in"," ",gsub('[[:digit:]]+', '',j)), pvalue = pv, nvalue=nv,status="", date =row2[1,dateCol],stringsAsFactors = FALSE)
      #   events <- rbind(events, e)
      # }
      # else{
      if(pv != nv)
      {
        # print(j)
        # print(as.numeric(nv))
        # print(as.numeric(pv))
        if(is.na(as.numeric(nv))||is.na(as.numeric(pv))){
          e <- data.frame(cId = cid, name=paste("Change in"," ",gsub('[[:digit:]]+', '',j)), pvalue = pv, nvalue=nv,status="", date =row2[1,dateCol],stringsAsFactors = FALSE)
          events <- rbind(events, e)
        }
        else{
          if(abs(as.numeric(nv)-as.numeric(pv))>delta){
            change = "+"
            changeStr = "Increase in "
            if((as.numeric(nv)-as.numeric(pv))<0){
              change = "-"
              changeStr= "Decrease in "
                  }
            e <- data.frame(cId = cid, name=paste(changeStr," ",gsub('[[:digit:]]+', '',j)),
                            pvalue = pv, nvalue=nv,status=change, date =row2[1,dateCol],stringsAsFactors = FALSE)
            events <- rbind(events, e)
          }
        }
      }
      # }
    }
    cid=cid+1
  }
  return(events)
}


### create transaction table 
### such that a transaction is represent the occurrence of an activity within case

createTransactions <- function(el) {
  
  
}


#### create EL from sensorData where an entry represent an event an a case represent a day

creatEL <- function(ds,dateCol) {
  splitDSday<-split(ds, as.Date(ds[,dateCol]))
  
}

createAcasePerday<- function(dataset,cId,dateCol) {
  colNames_ = colnames(dataset)
  colNames_ <-
    colNames_[lapply(colNames_, function(x)
      length(grep(dateCol, x, value = FALSE))) == 0]
  cId_ = 1
  events <- data.frame(cId = numeric(), name = character(), 
                       pvalue = character(), nvalue=character(), date = character(),stringsAsFactors = FALSE)
  for (i in 2:nrow(dataset)){
    row1 = dataset[i-1,]
    row2 = dataset[i,]
    for (j in colNames_) {
      pv = as.character(row1[1,j])
      nv = as.character(row2[1,j])
      
      if(pv != nv)
      {
        e <- data.frame(cId = cId, name=paste("Change in"," ",j),
                        pvalue = pv, nvalue=nv,date =row2[1,dateCol],stringsAsFactors = FALSE)
        events <- rbind(events, e)
      }
    }
  }
  return(events)
}


entryLog <- function(dataset, dataCol) {
  
  casesPerDay <-foreach(t = iter(splitDSday, by = 'row'), cid= 1:length(splitDSday), .combine = rbind) %do% {
      entryEvents(t,cid,"date")
    }
    return(casesPerDay)
}

entryEvents <-function (dataset, cid, dataCol="date"){
  colNames_ = colnames(dataset)
  colNames_ <-
    colNames_[lapply(colNames_, function(x)
      length(grep(dateCol, x, value = FALSE))) == 0]
  cId_ = 1
  events <- data.frame(cId = numeric(), name = character(), 
                       date = character(),stringsAsFactors = FALSE)
  
  for (j in colNames_) {
    if(is.numeric(dataset[,j])){
    events[, j] = numeric()
    }
    else {
      events[, j] = character()
    }
  }
  for (i in 1:nrow(dataset)){
    row2 = dataset[i,]
    e <- data.frame(cId = cId, name="Record",
                       ,stringsAsFactors = FALSE)
    for (j in colNames_) {
      e[, j] =row2[,j]
    }
    events <- rbind(events, e)
    
  }
  return(events)
}

EventPerEntryCasePerDay <- function(dataset,date) {
  
}