# Contents

This folder contains: 
1. Monthly Twitter-based indicators of unemployment in `./indicator/`
2. Programs necessary to produce them in `./code/`

# Monthly twitter-based indicators

Indicators are available for three countries: Brazil (`BR`), Mexico (`MX`), and United States (`US`):
- `./indicator/twitter_country.csv` contains indicators at the country-level
- `./indicator/twitter_city.csv` contains indicators at the top five cities for each country based on the total number of Twitter users

We use a BERT-based model with a threshold probability of 0.5 to classify the Twitter labels. 

Each file contains the following variables (in order): 
- `n_users` is the total number of Twitter users observed each month
- Twitter indicators named `pct_X` 
  - `X` denotes the different labels related to the Twitter users' unemployment status
  - Labels include `is_hired_1mo, `is_unemployed`, `job_offer`, `job_search`, and `lost_job_1mo`
- Twitter indicators for different sample splits of the Twitter users.
  - `pct_X_male` uses Twitter users whose gender is classified as male
  - `pct_X_female` uses Twitter users whose gender is classified as female
