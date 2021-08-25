** Created by: Do Lee dql204@nyu.edu
** Order of commands below. 

clear all
set more off
set matsize 10000 
set maxvar 30000
macro drop _all

cap mkdir ".\log\"
cap mkdir "..\indicator\"

do 1_sentiment_country.do
do 2_sentiment_local.do
do 3_sentiment_global.do
do 4_merge.do

*** End of File ***
