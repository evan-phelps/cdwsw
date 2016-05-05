--Update Patient IDs the Visit_detail table 
/* Currently tested the logic in the kbalak schema in dtdev */

merge into kbalak.visit_detail vis_d
using 
(select 
  distinct vd.visit_id, 
  pim.patient_id
from
  cdw.visit_detail vd,
  cdw.patient_id_map pim,
  cdw.visit v,
  kbalak.cdw_patient_tmp_20160320 incr
where vd.visit_id = v.visit_id
  and vd.htb_patient_id_ext = pim.mpi_lid
  and pim.mpi_euid = incr.mpi_euid
  and pim.mpi_systemcode = decode(v.htb_enc_id_root,
        '2.16.840.1.113883.3.2489.2.1.2.1.3.1.2.4', 'MUSC',
        '2.16.840.1.113883.3.2489.2.1.2.2.3.1.2.2', 'MUSC_EPIC',
        '2.16.840.1.113883.3.2489.2.2.2.1.3.1.2.4', 'GHS',
        '2.16.840.1.113883.3.2489.2.3.4.1.2.4.1', 'PH',
        '2.16.840.1.113883.3.2489.2.3.4.1.2.4.3', 'PH',
        '2.16.840.1.113883.3.2489.2.3.4.1.2.4.4', 'PH',
        '2.16.840.1.113883.3.2489.2.3.4.1.2.4.2', 'PH',
        '2.16.840.1.113883.3.2489.2.4.4.1.2.4.1', 'SRHS_R',
        '2.16.840.1.113883.3.2489.2.4.4.1.2.4.2', 'SRHS_S',
        '2.16.840.1.113883.3.2489.2.4.4.1.2.4.3', 'SRHS_V',
        NULL)
) recs 
on(vis_d.visit_id= recs.visit_id)
when matched then update set 
vis_d.patient_id=recs.patient_id
;


--Update Patient IDs the Visit table 
/* Currently tested the logic in the kbalak schema in dtdev */
merge into kbalak.visit vis
using 
(select 
  distinct vd.visit_id, 
  pim.patient_id
from
  cdw.visit_detail vd,
  cdw.patient_id_map pim,
  cdw.visit v,
  kbalak.cdw_patient_tmp_20160320 incr
where vd.visit_id = v.visit_id
  and vd.htb_patient_id_ext = pim.mpi_lid
  and pim.mpi_euid = incr.mpi_euid
  and pim.mpi_systemcode = decode(v.htb_enc_id_root,
        '2.16.840.1.113883.3.2489.2.1.2.1.3.1.2.4', 'MUSC',
        '2.16.840.1.113883.3.2489.2.1.2.2.3.1.2.2', 'MUSC_EPIC',
        '2.16.840.1.113883.3.2489.2.2.2.1.3.1.2.4', 'GHS',
        '2.16.840.1.113883.3.2489.2.3.4.1.2.4.1', 'PH',
        '2.16.840.1.113883.3.2489.2.3.4.1.2.4.3', 'PH',
        '2.16.840.1.113883.3.2489.2.3.4.1.2.4.4', 'PH',
        '2.16.840.1.113883.3.2489.2.3.4.1.2.4.2', 'PH',
        '2.16.840.1.113883.3.2489.2.4.4.1.2.4.1', 'SRHS_R',
        '2.16.840.1.113883.3.2489.2.4.4.1.2.4.2', 'SRHS_S',
        '2.16.840.1.113883.3.2489.2.4.4.1.2.4.3', 'SRHS_V',
        NULL)
) recs 
on(vis.visit_id= recs.visit_id)
when matched then update set 
vis.patient_id=recs.patient_id
;