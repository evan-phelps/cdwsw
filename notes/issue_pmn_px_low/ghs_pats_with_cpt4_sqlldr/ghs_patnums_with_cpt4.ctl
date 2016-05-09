load data 
infile 'ghs_patnums_with_cpt4.csv' "str '\n'"
append
into table ghs_pats_with_cpt4
fields terminated by ','
trailing nullcols
           ( lid CHAR(4000)
           )
