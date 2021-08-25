cap log close
loc logdate = string(d(`c(current_date)'), "%dNDY")
log using ".\log\3_sentiment_global_`logdate'.txt", text append
version 14
set linesize 225

********************************************************************************
*** GLOBAL NEWS SENTIMENT INDICATOR ESTIMATED FROM DYNAMIC FACTOR MODEL ***
********************************************************************************

use "..\indicator\sentiment_country.dta", clear

keep iso2 date sentiment_country

reshape wide sentiment_country, i(date) j(iso2) string

tsset date
tsfill

keep if year(date) > 1990 

ds sentiment_country*

loc vlist = "`r(varlist)'"

foreach vv in `vlist' {
	
	di "replace `vv' = 0 if mi(`vv')"
	replace `vv' = 0 if mi(`vv')
	
}
compress

dfactor ((`vlist') = , noconstant) (f = , ar(1/8))

ereturn list

predict sentiment_global if e(sample), factor smethod(filter)

compress
save "..\indicator\sentiment_global.dta", replace

cap log close
