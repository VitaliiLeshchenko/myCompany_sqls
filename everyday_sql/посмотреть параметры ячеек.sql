SELECT lh.area,
       LH.DSP_LOCN,
       LH.LOCN_CLASS,
       PLH.MAX_NBR_OF_SKU,
       PLH.REPL_FLAG,
       LH.PICK_DETRM_ZONE,
       PLH.PICK_LOCN_ASSIGN_ZONE,
       PLH.MAX_VOL,
       LH.LOCN_PICK_SEQ,
       LH.ZONE,
       LH.SKU_DEDCTN_TYPE,
       PLD.SKU_ID,
       PLH.*
  FROM LOCN_HDR LH
  JOIN PICK_LOCN_HDR PLH
    ON PLH.LOCN_ID = LH.LOCN_ID
  LEFT JOIN PICK_LOCN_DTL PLD
    ON PLD.LOCN_ID = LH.LOCN_ID
 WHERE
       (REGEXP_LIKE(lh.area, 'LB[B|D-V]'))
       AND REGEXP_LIKE(lh.lvl, '[1-3]')
       AND (LH.POSN BETWEEN '041' AND '137')
       AND MOD(LH.POSN, 4) IN ('1')
       --AND lh.locn_class = 'A'
       --AND plh.pick_locn_assign_zone NOT IN ('P00')
       --AND
       --to_char(plh.mod_date_time, 'YYYY-MM-DD HH24:MI:SS') > '2022-07-12 13:47:00'
       --AND PLH.MAX_NBR_OF_SKU = '9999'
       --plh.max_vol = 35
       --AND plh.user_id = 'VLESHCHENKO'
       --AND PLD.SKU_ID IS NOT NULL
 ORDER BY LH.DSP_LOCN;
