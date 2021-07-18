# Customer Churn Project
### Though we did not use any code to cleanse our data using code and plan to instead use glue data brew, if we needed to implement code to clean our data it would look something like this ...

df <- read.csv("C:/Users/joshr/Desktop/Old Quarters/Summer 2021/Big Data & Cloud/Excel files/BankChurners.csv")

### recode unknown categories as NA and omit them
df$CLIENTNUM <- NULL

df$Education_Level[df$Education_Level=="Unknown"] <- NA

df$Marital_Status[df$Marital_Status=="Unknown"] <- NA

df$Income_Category[df$Income_Category=="Unknown"] <- NA

df[!complete.cases(df),]

df2 <- na.omit(df)

### dummies
df2 <- dummy_cols(newdata, select_columns = c('Gender','Education_Level','Marital_Status','Income_Category','Card_Category'),remove_first_dummy = TRUE)

df2$Gender <- NULL

df2$Education_Level <- NULL

df2$Marital_Status <- NULL

df2$Income_Category <- NULL

df2$Card_Category <- NULL

### remove high correlations
df2$Avg_Open_To_Buy <- NULL

df2$Total_Trans_Ct <- NULL

df2$Avg_Utilization_Ratio <- NULL

df2$Total_Ct_Chng_Q4_Q1 <- NULL
