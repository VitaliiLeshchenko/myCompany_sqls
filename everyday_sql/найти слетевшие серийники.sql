SELECT "TABLE_NAME",
       "COLUMN_NAME",
       "ACTION",
       "SKU_ID",
       "OLD_VALUES",
       "NEW_VALUES",
       "DBUSER_ACTION",
       "OSUSER_ACTION",
       "WMUSER",
       "WORKSTATION",
       "PROGRAMID",
       "DATEACTION",
       "SIZE_DESC",
       "CD_MASTER_ID"
  FROM wmos.t_log_action
WHERE to_char (dateaction, 'YYYY-MM-DD') = '2022-07-12'
AND OLD_VALUES like '%DD237450%'
   AND UPPER(table_name) = 'SRL_NBR_TRACK';
