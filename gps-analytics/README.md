# GPS-analytics
### SDG-big-data
Using a dataset of individual GPS traces from mobile phone devices, we analyze for multiple countries behavioural changes caused by the COVID-19 
pandemic. In particular, we considered changes in mobility and time allocation across different socioeconomic groups.

This folder contains:
- The content of the book's chapter on GPS data analytics is stored in `bookdown`.
- The Apache Airflow pipeline used to run the code is stored in the folder `airflow_home`.
- The code used in the pipeline is in the folder `src`.
    - Shared and reusable code for GPS analysis is provided in the package `src/wbgps`.
    - The pipelines used that have been used for the analysis can be found in `src/pipelines`
    - Migration and mobility analysis code can be found in `src/analysis`.
    
