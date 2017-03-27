
spark_path <- strsplit(system("brew info apache-spark",intern=T)[4],' ')[[1]][1] # Get your spark path
.libPaths(c(file.path(spark_path,"libexec", "R", "lib"), .libPaths())) # Navigate to SparkR folder
library(SparkR) # Load the library

sparkR.session()

gnditem = read.df("/Users/Divya/OneDrive/Data Analytics/Spring 2017/DAEN 690/Data/TLD/GNDITEM.csv", 
                  'csv', header = "true", inferSchema = "true", na.strings = "NA")
loyalty_data = read.df("/Users/divya/OneDrive/Data Analytics/Spring 2017/DAEN 690/Data/Loyalty/Loyalty Rewards.csv", 
                  'csv', header = "true", inferSchema = "true", na.strings = "NA")

createOrReplaceTempView(gnditem, "table")

gnditem_filtered <- sql("Select * from table where PRICE > 0 AND QUANTITY == 1")

head(gnditem_filtered)
head(loyalty_data)

gnditem_filtered$CheckDate = concat(gnditem_filtered$DOB)

head(gnditem_filtered)
str(gnditem_filtered)
str(loyalty_data)

gnditem_filtered$CheckDate = date_format(gnditem_filtered$DOB, 'yyyy-MM-dd')
gnditem_filtered$CheckTime = concat_ws(sep =':', gnditem_filtered$HOUR,gnditem_filtered$MINUTE)
gnditem_filtered$CheckTime = date_format(gnditem_filtered$CheckTime, "HH:mm")
head(gnditem_filtered)

gnditem_filtered$ModDate <- concat_ws(sep=" ",gnditem_filtered$CheckDate,gnditem_filtered$CheckTime)#,":",gnditem_filtered$MINUTE))
head(gnditem_filtered)

gnditem_filtered$ModDate = date_format(gnditem_filtered$ModDate, 'yyyy-MM-dd HH:mm')
head(gnditem_filtered)

loyalty_data$ModDate = unix_timestamp(loyalty_data$ReceiptDate, 'MM/dd/yy HH:mm')
loyalty_data$ModDate = from_unixtime(loyalty_data$ModDate, 'yyyy-MM-dd HH:mm')
head(loyalty_data)
str(loyalty_data)

gnditem_filtered_new = select(gnditem_filtered, "dlTableStoreNumber", 'CHECK', "ITEM", "PARENT", "CATEGORY",
                          "MODE", "HOUR", "MINUTE", "PRICE", "DOB", "QUANTITY", "DISCPRIC", 'ModDate')
head(gnditem_filtered_new)

mergedDF = merge(gnditem_filtered_new,loyalty_data, by.x = c("dlTableStoreNumber" ,"CHECK", "ModDate"), 
                 by.y = c("StoreNumber", "ReceiptNumber", "ModDate") )
head(mergedDF)

#Merge Loyalty with GNDTNDR

gndtndr = read.df('C:/Users/Sriva/Desktop/GeorgeMason/Spring2017/DAEN690/R Scripts/src/TLD/GNDTNDR.csv','csv',header='true',inferSchema = 'true',na.strings='NA')

head(gndtndr)
head(loyalty_data)

createOrReplaceTempView(gndtndr, "table")
gndtndr_filtered <- sql("Select * from table where AMOUNT > 0")

nrow(gndtndr)
nrow(gndtndr_filtered)

gndtndr_filtered$dob <- date_format(gndtndr_filtered$DATE, 'YYYY-MM-dd')
gndtndr_filtered$time <- concat_ws(sep = ':',gndtndr_filtered$HOUR,gndtndr_filtered$MINUTE)
gndtndr_filtered$time <- date_format(gndtndr_filtered$time, 'HH:mm')
head(gndtndr_filtered)

gndtndr_filtered$Moddob <- concat_ws(sep = ' ',gndtndr_filtered$dob,gndtndr_filtered$time) 
head(gndtndr_filtered)

mergedDF_GNDTNDR_Loyalty = merge(gndtndr_filtered,loyalty_data, by.x = c("StoreNumber" ,"CHECK", "Moddob"), 
                                 by.y = c("StoreNumber", "ReceiptNumber", "ModDate") )
head(mergedDF_GNDTNDR_Loyalty)
nrow(mergedDF_GNDTNDR_Loyalty)

###mdf <- repartition(mergedDF_GNDTNDR_Loyalty,1)
#spark_write_csv(mergedDF_GNDTNDR_Loyalty, 'C:/Users/Sriva/Desktop/GeorgeMason/Spring2017/DAEN690/R Scripts/src/TLD/mergedDF_GNDTNDR_Loyalty.csv', header = TRUE, delimiter = ",",charset = "UTF-8", null_value = NULL,options = list())
#write.df(mdf,'C:/Users/Sriva/Desktop/GeorgeMason/Spring2017/DAEN690/R Scripts/src/TLD/mergedOutput',source = 'csv')

#write.csv(file = "mergedCSVV.csv",mergedDF_GNDTNDR_Loyalty)
#write.text(mergedDF_GNDTNDR_Loyalty, 'C:/Users/Sriva/Desktop/GeorgeMason/Spring2017/DAEN690/R Scripts/src/TLD/mergedOutput')




