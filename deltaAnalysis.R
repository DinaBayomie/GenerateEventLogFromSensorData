### statistical analysis over the sensor data 
##compute delta over sensor data
cDoSd <- function(sensor) {
  delta = list()
  j = 1
  if (!is.numeric(sensor)) {
    stop("Only numeric data allowed")
  }
 # if(anyNA(sensor)) {
  #  stop("There are NA in the data so the change cant be computed")
  #}
  for (i in 2:length(sensor)) {
   if(!is.na(sensor[i]) && !is.na(sensor[i-1]))
     delta[j] = sensor[i]-sensor[i-1]
    j=j+1
  }
  return(as.list(delta))
}


cStatD <- function(delta){
results = list(list())  
results[[1]] = summary(unlist(delta))

results[[2]] = mean(unlist(delta))
results[[3]] = sd(unlist(delta))

return(results)
}

computeStatsForSensorData<-function(sensorData){
  delta<-cDoSd(sensorData)
  stat<-cStatD(delta)
  return(stat)
}


computeForAllSensor<- function(dataset) {
  colNames_ = colnames(dataset)
  statResults <- list()
  i = 1
  for (j in colNames_) {
    if(is.numeric(dataset[,j])){
      statResults[[i]] = computeStatsForSensorData(dataset[,j])
      i = i + 1
    }
  }
  return(statResults)
  }













##### Change point detection ##### 

library(changepoint)
###Second, we need to identify penalty parameters for the algorithm we will use for testing
###changepoint detection against. For this example, we will be using the PELT algorithm. 
##We can do this by making 'elbow plots' and using the penalty parameter value at the elbow. 
###We will define a function cptfn for running through a sequence of different penalty parameters, 
###then plot for each time series:
cptfn <- function(data, pen) {
  ans <- cpt.mean(data, test.stat="Normal", method = "PELT", penalty = "Manual", pen.value = pen) 
  length(cpts(ans)) +1
}

# evaluate and plot results:
colNames_ = colnames(ds)
colNames_ <-
  colNames_[lapply(colNames_, function(x)
    length(grep("date", x, value = FALSE))) == 0]

plot.new()
frame()
par(mfcol=c(2,2))
pen.vals <- seq(0, 12,.2)
#plot_list = list()
#i = 1
for (c in colNames_[2:5]) {
  if(is.numeric(ds[,c]) && ! (anyNA(ds[,c]))){
    #print(c)
    
    elbowplotData <- unlist(lapply(pen.vals, function(p) 
      cptfn(data = ds[,c], pen = p)))
    #print(elbowplotData)
    plot.ts(ds[,c],type='l',col='red',
            xlab = "time",
            ylab = "y(t)",
            main = paste("Main signal:",c))
    # plot_list [[i]] <- recordPlot()  
    plot(pen.vals,elbowplotData, 
         xlab = paste("PELT penalty parameter : ",c),
         ylab = " ",
         main = " ")
    
    #plot_list[[i+1]] <- recordPlot() 
    #i = i +2
  }
  
}

# Identify changepoint in mean of signal 

penalty.val <- 6
cptm_stationary <- cpt.mean(y_ts,    penalty='Manual',pen.value=penalty.val,method='PELT') 
cpts_stationary <- cpts(cptm_stationary) # change point time points