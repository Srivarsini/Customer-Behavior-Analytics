#RF Clustering
#Considering RFM features file
library(ggplot2)
library(cluster) 
library(randomForest)
library(sqldf)
library(elasticnet)
library(MASS)
library(caret)

rfm_features = read.csv("/RFMFeatures.csv")
str(rfm_features)
head(rfm_features)
promotion_lbl = read.csv("/Crosstab of Cat_Item_Age_Sales With Promotion.csv")
names(promotion_lbl)[names(promotion_lbl) == 'User.Id'] <- 'CustomerID'
names(promotion_lbl)[names(promotion_lbl) == 'Discounts.Taken'] <- 'HasPromotion'


rfm_features = cbind(rfm_features, promotion_lbl[,13])

cleaned_rfm = rfm_features[ , -which(names(rfm_features) %in% c("FirstName", "LastName", "Gender", 
                                                                "FirstPurchaseDate", "LastPurchaseDate", "X"))]

#K-means clustering requires the data to be in numeric format
levels(cleaned_rfm$Generation)

cleaned_rfm$Generation <- gsub("Baby Boomers",0,cleaned_rfm$Generation)
cleaned_rfm$Generation <- gsub("Gen X",1,cleaned_rfm$Generation)
cleaned_rfm$Generation <- gsub("Gen Z",2,cleaned_rfm$Generation)
cleaned_rfm$Generation <- gsub("Millennials",3,cleaned_rfm$Generation)
cleaned_rfm$Generation <- gsub("Silent Generation",4,cleaned_rfm$Generation)

levels(cleaned_rfm$Region)
cleaned_rfm$Region <- gsub("Northeast",0,cleaned_rfm$Region)
cleaned_rfm$Region <- gsub("South",1,cleaned_rfm$Region)
cleaned_rfm$Region <- gsub("Midwest",2,cleaned_rfm$Region)
cleaned_rfm$Region <- gsub("West",3,cleaned_rfm$Region)

str(cleaned_rfm)
cleaned_rfm$Generation <- as.numeric(cleaned_rfm$Generation)
cleaned_rfm$Region <- as.numeric(cleaned_rfm$Region)
cleaned_rfm$UndiscountedTotalSale <- as.numeric(cleaned_rfm$UndiscountedTotalSale)
cleaned_rfm$TotalItems <- as.numeric(cleaned_rfm$TotalItems)
str(cleaned_rfm)
cleaned_rfm <- na.omit(cleaned_rfm)

head(cleaned_rfm)

#Cluster based on frequency and recency
cleaned_rfm$Recency <- (cleaned_rfm$Recency)/12 
set.seed(20)
freq_rec_cluster <- kmeans(cleaned_rfm[,c(2,4)], 6)
freq_rec_cluster
freq_rec_cluster$cluster <- as.factor(freq_rec_cluster$cluster)

#Creating ggplot of cluster
ggplot(cleaned_rfm, aes(cleaned_rfm$Frequency, cleaned_rfm$Recency, colour = freq_rec_cluster$cluster)) +
  geom_point()+ labs(title = "Frequency-Recency Cluster     k = 6", x = "Frequency", y = "Recency", color = "Legend Title\n") +
  scale_color_manual(labels = c("Lapsed", '',"High-Value",'','',''), values = c("1", "2","3","4","5",'6'))
cluster_fre_rec_output <- cbind(cleaned_rfm, freq_rec_cluster$cluster)

table(cluster_fre_rec_output$`freq_rec_cluster$cluster`)