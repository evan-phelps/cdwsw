create table dr105_cohort nologging as
select distinct
  pid,
  encid_t,
  encid_c,
  days_btwn
from
  (
    select /*+ parallel 4 */ distinct
      enc_cancer.patient_id pid,
      enc_trauma.visit_id encid_t,
      enc_cancer.visit_id encid_c,
      enc_cancer.visit_start_date - enc_trauma.visit_start_date days_btwn,
      rank() over ( partition by
                      enc_trauma.patient_id
                    order by
                      enc_trauma.visit_start_date desc,
                      enc_cancer.visit_start_date asc,
                      enc_trauma.visit_id desc,
                      enc_cancer.visit_id asc ) seq
    from cdw.diagnosis dx_trauma
    inner join cdw.visit enc_trauma
       on (    dx_trauma.visit_id = enc_trauma.visit_id )
    inner join cdw.visit enc_cancer
       on (    enc_trauma.patient_id = enc_cancer.patient_id
           and enc_trauma.visit_start_date <= enc_cancer.visit_start_date
           and enc_trauma.visit_start_date >= enc_cancer.visit_start_date-90 )
    inner join cdw.diagnosis dx_cancer
       on (    dx_cancer.visit_id = enc_cancer.visit_id )
    where ( (    dx_trauma.dx_code_type in ('ICD9','ICD-9-CM')
             and substr(dx_trauma.dx_code,1,2) in ('80','81','82','83',
                                         '84','85','86','87',
                                         '88','89','90','91',
                                         '92','93','94','95')
            )
         or (    dx_trauma.dx_code_type in ('ICD-10-CM')
             and substr(dx_trauma.dx_code,1,1) in ('S')
            )
         or (    dx_trauma.dx_code_type in ('ICD-10-CM')
             and substr(dx_trauma.dx_code,1,3) in ('T07','T14','T15','T16',
                                         'T17','T18','T19','T20',
                                         'T21','T22','T23','T24',
                                         'T25','T26','T27','T28',
                                         'T30','T31','T32','T33',
                                         'T34')
            )
          )
      and ( (    dx_cancer.dx_code_type in ('ICD9','ICD-9-CM')
             and substr(dx_cancer.dx_code,1,3) in ('182','183','184','162',
                                                   '163','164','165','149',
                                                   '150','151','152','153',
                                                   '154','155','156','157',
                                                   '158','159','174','175')
            )
         or (    dx_cancer.dx_code_type in ('ICD9','ICD-9-CM')
             and substr(dx_cancer.dx_code,1,5) in ('237.2')
            )
         or (    dx_cancer.dx_code_type in ('ICD-10-CM')
             and substr(dx_cancer.dx_code,1,3) in ('C51','C52','C53','C54',
                                                   'C55','C56','C57','C33',
                                                   'C34','C37','C38','C39',
                                                   'C14','C15','C16','C17',
                                                   'C18','C19','C20','C21',
                                                   'C22', 'C23','C24','C25',
                                                   'C26','C50','C74')
            )
          )
  )
where seq = 1
;

create table dr105_cohort_noprior nologging as
select /*+ parallel 4 */
  cohort.*
from dr105_cohort cohort
inner join cdw.visit enc on (    cohort.encid_t = enc.visit_id )
where not exists (
        select 1
        from cdw.visit enc_prior
        inner join cdw.diagnosis dx_prior
           on (    enc_prior.visit_id = dx_prior.visit_id )
        where enc_prior.patient_id = cohort.pid
          and enc_prior.visit_start_date < enc.visit_start_date
          and ( (    dx_prior.dx_code_type in ('ICD9','ICD-9-CM')
                 and substr(dx_prior.dx_code,1,3) in ('182','183','184','162',
                                                       '163','164','165','149',
                                                       '150','151','152','153',
                                                       '154','155','156','157',
                                                       '158','159','174','175')
                )
             or (    dx_prior.dx_code_type in ('ICD9','ICD-9-CM')
                 and substr(dx_prior.dx_code,1,5) in ('237.2')
                )
             or (    dx_prior.dx_code_type in ('ICD-10-CM')
                 and substr(dx_prior.dx_code,1,3) in ('C51','C52','C53','C54',
                                                       'C55','C56','C57','C33',
                                                       'C34','C37','C38','C39',
                                                       'C14','C15','C16','C17',
                                                       'C18','C19','C20','C21',
                                                       'C22', 'C23','C24','C25',
                                                       'C26','C50','C74')
                )
              )
        )
;

select
  c1.pid,
  c1.encid_t,
  c1.encid_c,
  round(c1.days_btwn) days_btwn,
  case when c2.pid is null then 'Y' else 'N' end prior_cancer_dx
from dr105_cohort c1
left outer join dr105_cohort_noprior c2
   on (    c1.pid = c2.pid )
order by pid
;

