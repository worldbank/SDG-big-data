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

The Makefile allow us to run the pipeline in a seamlessly way in a local or web based infrastructure. We only need to provide a Spark environment.

We made a Python package that can be installed in a local directory that contains the needed functions to handle every step of the pipeline.

In order to prepare the local environment for execution we only need to execute:

```
make init
```

which will prepare the required Python version in an isolated environment. Then we execute:

```
make deps
```

in order to install the requirements and our *wbgps* Python library and  define the environmental variables for the Airflow pipeline.

The pipeline is managed using [Apache Airflow](https://airflow.apache.org/) so it will run sequentially in the desired order. There's only one parameter
that we currently have to write manually, which is the desired country. Then if we want to run the entire pipeline for Mexico, then we would only need to submit
the following command with the 2 character code for Mexico which is 'MX':

```
make country="MX" runpipeline
```

The other arguments have been already optimized in the optimization part of this study and are hardcoded in the _Airflow_ DAGs, but they can be overwritten if desired.