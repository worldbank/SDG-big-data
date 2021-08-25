rm(list = ls())
getwd()
set.seed(48935664)
suppressWarnings(dir.create("./out/"))

# Install packages not yet installed

packages = c("pacman", "plyr", "dplyr", "tidyr")

installed_packages = packages %in% rownames(installed.packages())

if (any(installed_packages==FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading

invisible(lapply(packages, library, character.only = TRUE))

#####################################################

# List of countries

clist = c("MX", "BR", "US")

# List of twitter models 

models = c(
  # "keywords", # Keyword-based model 
  "bert05" # , # BERT cutoff prob 0.5 model
  # "bert09" # BERT cutoff prob 0.9 model
)

# List of different splits of the Twitter users

splits = c(
  "total", # all accounts
  "gender" # , # gender
  # "ndays", # account age
  # "ntweets" # account activity
) 
split_name = c(
  "total", # all accounts
  "gender" # , # male/female/unknown
  # "ndays", # Q1-Q10 deciles of account age 
  # "ntweets" # Q1-Q10 deciles of account activity 
)
groups = list(
  total = "", 
  gender = c("male","female") #, # ,"unknown"
  # ndays = c("L", "M", "H"), # Low/Medium/High account age
  # ntweets = c("L", "M", "H") # Low/Medium/High account activity
)

# List of twitter labels

labels = c(
  "is_hired_1mo", "is_unemployed", "job_offer", 
  "job_search", "lost_job_1mo"
)

# List of twitter labels as share of n_users

labels_n_users = paste0(paste0(labels, "_", "n_users"))

#####################################################

# Country-level data

#####################################################

# Stack country-level data into panel

cnt = 1
for (iso2 in clist) {
  
  tmp_data = read.csv(
    file = paste0("../../code/out/data_iso2_", iso2, ".csv"), 
    header = TRUE, sep = ",", 
    stringsAsFactors = FALSE, fileEncoding = "UTF-8")
  
  if (cnt==1) {
    data_iso2 = tmp_data
  } else {
    data_iso2 = rbind(data_iso2, tmp_data)
  }
  cnt = cnt + 1
}
data_iso2$X = NULL

data_iso2$country = data_iso2$iso2
data_iso2[data_iso2$country=="BR",]$country = "Brazil"
data_iso2[data_iso2$country=="MX",]$country = "Mexico"
data_iso2[data_iso2$country=="US",]$country = "United States"

data_iso2 = data_iso2[
  ,c("country","year","month","n_users_bert05_total",#"iso2",
     paste0(labels_n_users, "_bert05_total"), 
     paste0(labels_n_users, "_bert05_gender_male"), 
     paste0(labels_n_users, "_bert05_gender_female"))
  ] %>% arrange(country,year,month)

colnames(data_iso2) = c(
  "country","year","month","n_users",#"iso2",
  paste0("pct_",labels),
  paste0("pct_",labels,"_male"),
  paste0("pct_",labels,"_female"))

write.csv(data_iso2, 
          file = paste0("../indicator/twitter_country", ".csv"), 
          row.names = FALSE)

#####################################################

# City-level data

#####################################################

# Stack city-level data into panel

data_city = list()

cnt = 1
for (iso2 in clist) {
  
  if (iso2=="US") {
    tmp_data = read.csv(
      file = paste0("../../code/out/data_city_", iso2, ".csv"), 
      header = TRUE, sep = ",", 
      stringsAsFactors = FALSE)
  } else {
    tmp_data = read.csv(
      file = paste0("../../code/out/data_city_", iso2, ".csv"), 
      header = TRUE, sep = ",", 
      stringsAsFactors = FALSE, fileEncoding = "UTF-8")
  }
  
  if (cnt==1) {
    data_city = tmp_data
  } else {
    data_city = rbind(data_city, tmp_data)
  }
  cnt = cnt + 1
}
data_city$X = NULL

data_city$country = data_city$iso2
data_city[data_city$country=="BR",]$country = "Brazil"
data_city[data_city$country=="MX",]$country = "Mexico"
data_city[data_city$country=="US",]$country = "United States"

# Total n_users by city

names_city_n_users = c(
  "iso2","geo_id","metro_area_name",
  "n_users_keywords_total")

city_n_users = data_city[which(!is.na(data_city$unrate)),
                         names_city_n_users] %>% 
  group_by(iso2, geo_id, metro_area_name) %>% 
  summarise_all(sum, na.rm = TRUE) %>% 
  arrange(desc(n_users_keywords_total)) %>% as.data.frame()

# List of Top 3 cities sorted by n_users

list_top3 = c()

for (iso2 in clist) {
  
  tmp_city_n_users = city_n_users[city_n_users$iso2==iso2,]
  list_top3 = c(list_top3, tmp_city_n_users[c(1:3),"metro_area_name"])
}

var_name = c()

for (m in models) {
  for (s in 1:length(splits)) {
    for (var in labels_n_users) {
      
      tmp_var = paste0(var, "_", m, "_", splits[s])
      
      if (split_name[s]=="total") {
        var_name = c(var_name, tmp_var)
      } else {
        var_name = c(var_name, paste0(
          paste0(tmp_var, "_"), groups[[s]])
        )
      }
    }
  }
}

data_city = data_city[
  data_city$metro_area_name %in% list_top3,
  c("country","metro_area_name","year","month","n_users_bert05_total",#"iso2",
    paste0(labels_n_users, "_bert05_total"),
    paste0(labels_n_users, "_bert05_gender_male"), 
    paste0(labels_n_users, "_bert05_gender_female"))
  ] %>% arrange(country,metro_area_name,year,month)

colnames(data_city) = c(
  "country","metro_area_name","year","month","n_users",#"iso2",
  paste0("pct_",labels),
  paste0("pct_",labels,"_male"),
  paste0("pct_",labels,"_female"))

write.csv(data_city, 
          file = paste0("../indicator/twitter_city", ".csv"), 
          row.names = FALSE)

