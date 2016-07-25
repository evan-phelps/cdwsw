MERGE INTO cdw.vital vit
  USING
  (
    SELECT
      p.source_act_id AS htb_enc_act_id,
      p.source_act_ver_num AS htb_enc_act_ver_num,
      p.target_act_id AS htb_po_act_id,
      p.target_act_ver_num AS htb_po_act_ver_num,
      v.concept_code AS src_code,
      act.ob_value_type_code AS vital_value_type,
      act.ob_value_txt AS vital_value_num,
      act.ob_value_date_ts AS vital_value_date,
      act.ob_value_st_txt AS vital_value_string,
      act.ob_value_uom_code AS vital_value_unit,
      act.effective_start_date_ts AS collection_date,
      act.last_update_date AS last_update_date,
      14 AS datasource_id,
      DECODE(v.concept_code, '3137-7', 'HEIGHT',
         '8335-2', 'WEIGHT',
         '8339-4', 'WEIGHT',
         'UNK') observation_type,
      'SRHS' AS institution
    FROM ctb_core_act_relations@HTB_APPS p
    INNER JOIN ctb_core_acts@HTB_APPS act
       ON (    act.act_id = p.target_act_id
           AND act.act_version_num = p.target_act_ver_num)
    LEFT JOIN hct_et_concepts@HTB_APPS c
       ON (c.concept_id = act.ob_value_code_ets_id)
    INNER JOIN hct_et_concepts@HTB_APPS v
       ON (    v.concept_id = act.act_code_ets_id
           AND v.concept_code IN
             ('3137-7',
              '8335-2',
              '8339-4')
       )
    INNER JOIN cdw.visit enc
       on (     p.source_act_id = enc.visit_id
            and enc.datasource_id = 14
            and enc.last_update_date > to_date('2016-04-06', 'YYYY-MM-DD')
                                )
    WHERE p.type_code = 'PERT'
      AND act.class_code = 'OBS'
      AND act.mood_code = 'EVN'
      AND act.act_code_ets_id IN
            ('CON-196238',
             'CON-232344',
             'CON-232348')
      AND act.current_version_flag = 'Y'
  ) htbvit ON ( vit.htb_po_act_id = htbvit.htb_po_act_id )
  when matched then update set
    vit.htb_enc_act_ver_num = htbvit.htb_enc_act_ver_num,
    vit.htb_po_act_ver_num = htbvit.htb_po_act_ver_num,
    vit.src_code = htbvit.src_code,
    vit.vital_value_type = htbvit.vital_value_type,
    vit.vital_value_num = htbvit.vital_value_num,
    vit.vital_value_date = htbvit.vital_value_date,
    vit.vital_value_string = htbvit.vital_value_string,
    vit.vital_value_unit = htbvit.vital_value_unit,
    vit.collection_date = htbvit.collection_date,
    vit.last_update_date = htbvit.last_update_date,
    vit.datasource_id = htbvit.datasource_id,
    vit.observation_type = htbvit.observation_type,
    vit.institution = htbvit.institution
  where htbvit.htb_po_act_ver_num > vit.htb_po_act_ver_num
  when not matched then insert
  (
    vit.htb_enc_act_id,
    vit.htb_enc_act_ver_num,
    vit.htb_po_act_id,
    vit.htb_po_act_ver_num,
    vit.src_code,
    vit.vital_value_type,
    vit.vital_value_num,
    vit.vital_value_date,
    vit.vital_value_string,
    vit.vital_value_unit,
    vit.collection_date,
    vit.last_update_date,
    vit.datasource_id,
    vit.observation_type,
    vit.institution
  ) values (
    htbvit.htb_enc_act_id,
    htbvit.htb_enc_act_ver_num,
    htbvit.htb_po_act_id,
    htbvit.htb_po_act_ver_num,
    htbvit.src_code,
    htbvit.vital_value_type,
    htbvit.vital_value_num,
    htbvit.vital_value_date,
    htbvit.vital_value_string,
    htbvit.vital_value_unit,
    htbvit.collection_date,
    htbvit.last_update_date,
    htbvit.datasource_id,
    htbvit.observation_type,
    htbvit.institution
  )
;
commit;

