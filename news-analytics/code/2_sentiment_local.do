cap log close
loc logdate = string(d(`c(current_date)'), "%dNDY")
log using ".\log\2_sentiment_local_`logdate'.txt", text append
version 14
set linesize 225

cap mkdir "..\indicator"

********************************************************************************
*** LOCAL NEWS SENTIMENT INDICATOR ***
********************************************************************************

insheet using "..\..\replication\data\features-1991-1996-missing-countries.csv", comma clear
compress
drop v1
tempfile _tmp
save `_tmp'

insheet using "..\..\replication\data\news.csv", comma clear
compress
d
append using `_tmp', gen(source)

* Tags of topic-specific articles

cap ren *commodity* *commdty*
cap ren *monetary* *money*
cap ren *external* *extrnl*
cap ren *political* *polit*
cap ren *general* *gen*
cap ren *policy* *pol*
cap ren *performance* *perform*
cap ren *corporate* *corp*
cap ren *derivative* *deriv*
cap ren *market* *mkt*
cap ren *economic* *ecn*
cap ren *government* *govt*
cap ren *indicators* *ind*
cap ren *financial* *fin*
cap ren *payments* *pay*
cap ren *securities* *scrty*
cap ren *industrial* *indl*
cap ren *forex* *fx*
cap ren *pay* **
cap ren *mkts* *mkt*
cap ren *bond* **
cap ren *perform* **
cap ren *commdty* *comm*
cap ren *finance* *fin*

* Local baseline sentiment excluding topics with no signal

gen sentiment_local = positive - negative if local==1 & ///
	!(table==1 | tables==1 | tradeextrnl==1 | ecnind==1 | debtmkt==1)

gen n_articles_local = (words!=0) & local==1 & ///
	!(table==1 | tables==1 | tradeextrnl==1 | ecnind==1 | debtmkt==1)

ren date str_date
gen date = date(str_date, "YMD", 2000), after(str_date)
format date %tdnn/dd/CCYY

keep country date sentiment* n_articles*

collapse (mean) sentiment* (sum) n_articles*, by(country date)

compress

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

save "..\indicator\sentiment_local.dta", replace

cap log close
