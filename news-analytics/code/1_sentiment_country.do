cap log close
loc logdate = string(d(`c(current_date)'), "%dNDY")
log using ".\log\1_sentiment_country_`logdate'.txt", text append
version 14
set linesize 225

********************************************************************************
*** COUNTRY-SPECIFIC NEWS SENTIMENT INDICATOR ***
********************************************************************************

insheet using ///
	"..\..\replication\data\sentiments-panel-reuters-econ-fin-mkt-25-countries-1991-2019.csv" ///
	, comma clear
	
ren v1 country

gen sentiment_country = positive - negative
gen n_articles_country = 1 if words!=0

keep country year month day sentiment_country n_articles

collapse (mean) sentiment* (sum) n_articles*, by(country year month day)

compress

gen date = mdy(month, day, year)
format date %tdnn/dd/CCYY

gen iso2 = "", after(country)
replace iso2 = "AR" if country=="Argentina"
replace iso2 = "BR" if country=="Brazil"
replace iso2 = "CL" if country=="Chile"
replace iso2 = "CN" if country=="China"
replace iso2 = "FR" if country=="France"
replace iso2 = "DE" if country=="Germany"
replace iso2 = "GR" if country=="Greece"
replace iso2 = "IN" if country=="India"
replace iso2 = "ID" if country=="Indonesia"
replace iso2 = "IE" if country=="Ireland"
replace iso2 = "IT" if country=="Italy"
replace iso2 = "JP" if country=="Japan"
replace iso2 = "KR" if country=="Korea"
replace iso2 = "MY" if country=="Malaysia"
replace iso2 = "MX" if country=="Mexico"
replace iso2 = "PE" if country=="Peru"
replace iso2 = "PH" if country=="Philippines"
replace iso2 = "PL" if country=="Poland"
replace iso2 = "PT" if country=="Portugal"
replace iso2 = "RU" if country=="Russia"
replace iso2 = "ZA" if country=="South Africa"
replace iso2 = "KR" if country=="South Korea"
replace iso2 = "ES" if country=="Spain"
replace iso2 = "TH" if country=="Thailand"
replace iso2 = "TR" if country=="Turkey"
replace iso2 = "US" if country=="United States"
	tab iso2, m

compress
d

save "..\indicators\sentiment_country.dta", replace

cap log close
