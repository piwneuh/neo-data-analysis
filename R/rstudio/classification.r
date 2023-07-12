install.packages("tidyverse", repos = "https://cran.rstudio.com/")
install.packages("sparklyr", repos="https://cran.rstudio.com")

library(sparklyr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(magrittr)
library(knitr)

# Install
spark_install()

# Connect
sc <- sparklyr::spark_connect(master = "local")

datasetPath <- "/data/dataset.csv"
df <- spark_read_csv(sc, name = "neo_data", path = datasetPath, header = TRUE, infer_schema = TRUE)

# Filter data
df <- df %>%
  filter(!(is.na(H) || is.na(epoch) || is.na(diameter) || is.na(diameter_sigma))) %>%
  mutate(albedo = ifelse(is.na(albedo), 0, albedo))

# Check how much is lost
count(df)

# Create a new column that categorizes asteroids into different brightness categories based on their absolute magnitude (H)
df <- df %>%
  mutate(brightness = case_when(
    albedo < 0.25 ~ "Can't be seen",
    albedo >= 0.25 ~ "Moderately Bright",
    TRUE ~ "Can't be seen"
  ))

# Create a new column 
df <- df %>%
  mutate(can_be_seen = case_when(
    albedo < 0.25 ~ 0,
    albedo >= 0.25 ~ 1,
    TRUE ~ 0
  ))

# H devided by 5 (needed for formula)
df <- df %>%
  mutate(H_divd = H / 5)

# Sanity columns
colnames(df)

# Select most relevant data for further analising
df <- df %>% select(albedo, H, H_divd, diameter, diameter_sigma, epoch, can_be_seen, brightness)

# Just in case
df <- na.omit(df)

# Show selected data
kable(head(df, n = 10L),
      col.names = colnames(df),
      caption = "Table view of filtered data",
      format = "html",
      align = "c"
)

# Data split
df_split <- sdf_random_split(df, training = 0.7, test = 0.3)

# Check the split out
df_split

# Formula
formula <- can_be_seen ~ H + H_divd + diameter + albedo

# Logistic regression with different max iterations
max_iterations <- 5
# Create empty data frame
result_data <- data.frame(
  i = 1:max_iterations, 
  wp = numeric(max_iterations), 
  wr = numeric(max_iterations), 
  a = numeric(max_iterations),
  f1 = numeric(max_iterations)
  )

for (i in 1:max_iterations) {
  # Traing the model
  model <- ml_logistic_regression(df_split$training, formula, max_iter = i, family = "binomial")
  
  # Evaluate the model
  result <- ml_evaluate(model, df_split$test)
  
  # Save results
  result_data$wp[i] <- result$weighted_precision()
  result_data$wr[i] <- result$weighted_recall()
  result_data$a[i] <- result$accuracy()
  result_data$f1[i] <- result$weighted_f_measure()
}

# Sanity
result_data

# Plotting Precision
plot1 <- result_data %>%
  ggplot(aes(i, wp, color = wp)) +
  geom_line(linewidth = 2) +
  scale_x_continuous(breaks = 1:max_iterations) +
  scale_y_continuous(breaks = result_data$wp) +
  scale_color_gradient(low = "#FF2266", high = "#6622FF") +
  theme(text = element_text(size = 16)) +
  labs(x = "Num of iterations", y = "Precision", title = "a) Precision dependency on number of iterations")

# Plotting Recall
plot2 <- result_data %>%
  ggplot(aes(i, wp, color = wp)) +
  geom_line(linewidth = 2) +
  scale_x_continuous(breaks = 1:max_iterations) +
  scale_y_continuous(breaks = result_data$wr) +
  scale_color_gradient(low = "#FF2266", high = "#6622FF") +
  theme(text = element_text(size = 16)) +
  labs(x = "Num of iterations", y = "Recall", title = "a) Recall dependency on number of iterations")

# Plotting Accuracy
plot3 <- result_data %>%
  ggplot(aes(i, wp, color = wp)) +
  geom_line(linewidth = 2) +
  scale_x_continuous(breaks = 1:max_iterations) +
  scale_y_continuous(breaks = result_data$a) +
  scale_color_gradient(low = "#FF2266", high = "#6622FF") +
  theme(text = element_text(size = 16)) +
  labs(x = "Num of iterations", y = "Accuracy", title = "a) Accuracy dependency on number of iterations")

# Plotting F1
plot4 <- result_data %>%
  ggplot(aes(i, wp, color = wp)) +
  geom_line(linewidth = 2) +
  scale_x_continuous(breaks = 1:max_iterations) +
  scale_y_continuous(breaks = result_data$f1) +
  scale_color_gradient(low = "#FF2266", high = "#6622FF") +
  theme(text = element_text(size = 16)) +
  labs(x = "Num of iterations", y = "F1", title = "a) F1 score dependency on number of iterations")

# Display
plot1
plot2
plot3
plot4

# Reaffirm formula
formula <- can_be_seen ~ H + H_divd + diameter + albedo

# Bayes classification
bayes_model <- df_split$training %>% ml_naive_bayes(formula)

# Linear SVC classification
linear_svc_model <- df_split$training %>%
  ml_linear_svc(formula)

# Decision 3 classification
decision_tree_classifier <- df_split$training %>%
  ml_decision_tree_classifier(formula)

# Testing Accuracy Score for different methods using test dataset
bayes_accuracy <- ml_evaluate(bayes_model, df_split$test)$Accuracy
svc_accuracy <- ml_evaluate(linear_svc_model, df_split$test)$Accuracy
d3_accuracy <- ml_evaluate(decision_tree_classifier, df_split$test)$Accuracy

# Testing Accuracy Score for different methods using 4-cross validation
k4c <- function(dataset, model, formula){
  dataset <- dataset %>%
    sdf_random_split(seed=1,
                     s1=0.25,
                     s2=0.25,
                     s3=0.25,
                     s4=0.25)
  training <- list(
    s1 = sdf_bind_rows(dataset$s2, dataset$s3, dataset$s4),
    s2 = sdf_bind_rows(dataset$s1, dataset$s3, dataset$s4),
    s3 = sdf_bind_rows(dataset$s1, dataset$s2, dataset$s4),
    s4 = sdf_bind_rows(dataset$s1, dataset$s2, dataset$s3)
  )
  
  trained = list(s1=model(training$s1, formula),
                 s2=model(training$s2, formula),
                 s3=model(training$s3, formula),
                 s4=model(training$s4, formula)
  )
  
  accuracy <- (ml_evaluate(trained$s1, dataset$s1)$Accuracy +
                    ml_evaluate(trained$s2, dataset$s2)$Accuracy +
                    ml_evaluate(trained$s3, dataset$s3)$Accuracy +
                    ml_evaluate(trained$s4, dataset$s4)$Accuracy
  ) / 4
}

bayes_k4_accuracy <- k4c(df, ml_naive_bayes, formula)
svc_k4_accuracy <- k4c(df, ml_linear_svc, formula)
d3_k4_accuracy <- k4c(df, ml_decision_tree_classifier, formula)

# Table View
knitr::kable(array(c("Bayes", "SVC", "Decision Tree",
                     bayes_accuracy, svc_accuracy, d3_accuracy,
                     bayes_k4_accuracy, svc_k4_accuracy, d3_accuracy),
                   dim = c(3, 3)),
             col.names = c("Models", "Accuracy", "4-Cross Accuracy"),
             label = "Comparing accuracy of different models",
             align = "ccc",
             format = "html"
)

# Clusterizatio dataset
df_clusterization <- spark_read_csv(sc, name = "neo_data", path = datasetPath, header = TRUE, infer_schema = TRUE)

# Filter clusterization data
df_clusterization <- df_clusterization %>%
  filter(!(is.na(diameter) || is.na(e)))

# Create a new column that categorizes asteroids into different size categories based on their diameter (km)
df_clusterization <- df_clusterization %>%
  mutate(size_category = case_when(
    diameter < 1 ~ "Small",
    diameter >= 1 & diameter < 5 ~ "Medium",
    diameter >= 5 & diameter < 10 ~ "Large",
    diameter >= 10 ~ "X-Large",
    TRUE ~ "Unknown"
  ))

# Select columns relevant for clusterizations
df_clusterization <- df_clusterization %>% select(diameter, e, size_category)

# New Formula
cluster_formula <- size_category ~ diameter + e

# K-means 5
model_5 <- ml_kmeans(df_clusterization, cluster_formula, seed = 1, k = 5)

cplot_5 <- model_5$centers %>%
  ggplot(aes(diameter, e, color=e)) +
  geom_point(size=5) +
  scale_color_gradientn(colors = rainbow(10)) +
  theme(text = element_text(size=16)) +
  labs(x="diameter", y="number of samples", title = "a) K=5-means")

model_10 <- ml_kmeans(df_clusterization, cluster_formula, seed = 1, k = 10)

cplot_10 <- model_10$centers %>%
  ggplot(aes(diameter, e, color=e)) +
  geom_point(size=5) +
  scale_color_gradientn(colors = rainbow(10)) +
  theme(text = element_text(size=16)) +
  labs(x="diameter", y="number of samples", title = "a) K=10-means")

model_20 <- ml_kmeans(df_clusterization, cluster_formula, seed = 1, k = 20)

cplot_20 <- model_20$centers %>%
  ggplot(aes(diameter, e, color=e)) +
  geom_point(size=5) +
  scale_color_gradientn(colors = rainbow(10)) +
  theme(text = element_text(size=16)) +
  labs(x="diameter", y="number of samples", title = "a) K=20-means")

# Plotin'
cplot_5
cplot_10
cplot_20

# End the session
#spark_disconnect(sc)
