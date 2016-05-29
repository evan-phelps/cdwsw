create table cdw.hssc_bmi nologging as
/* Assumes heights and weights are in cm and kg,
    as per the current height/weight kludge */
select /*+ parallel 4 */
  patient_id,
  htb_enc_act_id,
  htb_enc_act_ver_num,
  institution,
  datasource_id,
  'HSSC_BMI_ADULT' observation_type,
  collection_date, /* set by WEIGHT observation date */
  round(10000*wt_val/(ht_val*ht_val),1) bmi_val,
  'kg/m^2' bmi_uom,
  sysdate last_update_date
from
  (
    select
      pat.patient_id,
      weight.htb_enc_act_id,
      weight.htb_enc_act_ver_num,
      weight.institution,
      enc_wt.datasource_id,
      coalesce(weight.collection_date, enc_wt.visit_start_date) collection_date,
      pat_latest_ht.ht ht_val,
      pat_latest_ht.uom ht_uom,
      weight.vital_value_num wt_val,
      weight.vital_value_unit wt_uom,
      /* first weight measurement in encounter should be used,
         seq_within_enc = 1 */
      rank() over ( partition by enc_wt.visit_id
                    order by weight.collection_date,
                             weight.htb_po_act_id,
                             weight.htb_po_act_ver_num
                  ) seq_within_enc
    from cdw.vital weight
    inner join cdw.visit enc_wt on ( weight.htb_enc_act_id = enc_wt.visit_id )
    inner join cdw.patient pat on ( enc_wt.patient_id = pat.patient_id )
    inner join (
      /* latest height for adult patient can be used,
         because since height doesn't change much in adulthood */
      select * from
      (
        select
          pat2.patient_id,
          height2.vital_value_num ht,
          height2.vital_value_unit uom,
          rank() over ( partition by pat2.patient_id
                        order by enc2.visit_start_date desc,
                                 height2.collection_date desc
                      ) seq_rev
        from cdw.vital height2
        inner join cdw.visit enc2 on ( height2.htb_enc_act_id = enc2.visit_id )
        inner join cdw.patient pat2 on ( enc2.patient_id = pat2.patient_id )
        where height2.observation_type = 'HEIGHT'
          /* adults are over 20 years old */
          and enc2.visit_start_date - pat2.birth_date >= 20*365.25
          /* height in cm; range to cut out outlying data */
          and height2.vital_value_num > 100 and height2.vital_value_num < 300
      ) where seq_rev = 1
    ) pat_latest_ht on ( pat.patient_id = pat_latest_ht.patient_id )
    where weight.observation_type = 'WEIGHT'
      /* adults are over 20 years old */
      and enc_wt.visit_start_date - pat.birth_date >= 20*365.25
      /* weight in kg, range to cut out outlying data */
      and weight.vital_value_num > 10 and weight.vital_value_num < 650
  )
/* first weight measurement of encounter */
where seq_within_enc = 1
;