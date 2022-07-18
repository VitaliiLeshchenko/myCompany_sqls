CREATE OR REPLACE VIEW WMS_PKT_8068_200929 AS
SELECT DISTINCT
           CASE
               WHEN im.cd_master_id = '18005' THEN '00027'
               WHEN im.cd_master_id = '18004' THEN '00026'
               WHEN im.cd_master_id = '10004' THEN '00017'
               WHEN im.cd_master_id = '11004' THEN '00018'
               WHEN im.cd_master_id = '6003' THEN '00009'
               WHEN im.cd_master_id = '4003' THEN '00005'
               WHEN im.cd_master_id = '5004' THEN '00008'
               WHEN im.cd_master_id = '9004' THEN '00013'
               WHEN im.cd_master_id = '9005' THEN '00014'
               WHEN im.cd_master_id = '9006' THEN '00015'
               WHEN im.cd_master_id = '5003' THEN '00007'
               WHEN im.cd_master_id = '12004' THEN '00020'
               WHEN im.cd_master_id = '17004' THEN '00025'
               WHEN im.cd_master_id = '13004' THEN '00021'
               WHEN im.cd_master_id = '15004' THEN '00023'
               WHEN im.cd_master_id = '3001' THEN '00003'
               WHEN im.cd_master_id = '3003' THEN '00004'
               WHEN im.cd_master_id = '10003' THEN '00016'
               ELSE ' '
           END                                              "client_id",
           CASE
               WHEN im.cd_master_id = '18005'
               THEN
                   'ORBICO'
               WHEN im.cd_master_id = '18004'
               THEN
                   'AUTOTECH'
               WHEN im.cd_master_id = '10004'
               THEN
                   'Грейс ПРО'
               WHEN im.cd_master_id = '11004'
               THEN
                   'Karma D'
               WHEN im.cd_master_id = '6003'
               THEN
                   'Protoria'
               WHEN im.cd_master_id = '4003'
               THEN
                   'БНС Трэйд'
               WHEN im.cd_master_id = '5004'
               THEN
                   'Fors'
               WHEN im.cd_master_id = '9004'
               THEN
                   'Sialed Air'
               WHEN im.cd_master_id = '9005'
               THEN
                   'TEKA'
               WHEN im.cd_master_id = '9006'
               THEN
                   'Legrand'
               WHEN im.cd_master_id = '5003'
               THEN
                   'ДЕЙЛІ ЧОЙС'
               WHEN im.cd_master_id = '12004'
               THEN
                   'БРЕНД СТАЙЛ'
               WHEN im.cd_master_id = '17004'
               THEN
                   'БіоЛайн'
               WHEN im.cd_master_id = '13004'
               THEN
                   'Коттон'
               WHEN im.cd_master_id = '15004'
               THEN
                   'Смартмакс'
               WHEN im.cd_master_id = '3001'
               THEN
                   'MIX'
               WHEN im.cd_master_id = '3003'
               THEN
                   'Київмедпрепарат'
               WHEN im.cd_master_id = '10003'
               THEN
                   'UPECO'
               ELSE
                   ' '
           END                                              "client_name",
           pi.stat_code,
           ph.pkt_ctrl_nbr                                  AS unique_id,
           NVL (ph.pro_nbr, ph.pkt_ctrl_nbr)                AS pkt_nbr,
           ph.terms_code,
           ph.shipto_addr_3                                 AS TTN,
           ph.shipto,
           ph.shipto_city,
           ph.shipto_zip,
           ph.shipto_name,
           ph.shipto_addr_1,
           ph.shipto_addr_2,
           ph.shipto_contact,
           ph.soldto,
           ph.soldto_name,
           ph.soldto_addr_1,
           pd.pkt_seq_nbr,
           im.size_desc                                     AS sku_wms,
           im.vendor_item_nbr                               AS sku_client,
           pd.orig_pkt_qty,
           (SELECT SUM (ccd.units_pakd)
              FROM carton_dtl ccd
             WHERE     ccd.carton_nbr = cd.carton_nbr
                   AND ccd.pkt_seq_nbr = cd.pkt_seq_nbr)    AS units_pakd,
           im.sku_brcd,
           pd.prod_stat,
           pd.batch_nbr,
           im.sku_desc,
           pd.price,
           pd.retail_price,
           pd.unit_monetary_value                           AS unit_price,
           im.unit_ht,
           im.unit_len,
           im.unit_width,
           im.unit_wt,
           ch.carton_nbr,
           pi.mod_date_time,
           ph.plan_load_nbr                                 AS load_nbr -- 11.10.2021
      FROM pkt_hdr  ph
           JOIN pkt_dtl pd ON pd.pkt_ctrl_nbr = ph.pkt_ctrl_nbr
           JOIN pkt_hdr_intrnl pi ON pi.pkt_ctrl_nbr = ph.pkt_ctrl_nbr
           JOIN item_master im ON im.sku_id = pd.sku_id
           JOIN carton_hdr ch ON ch.pkt_ctrl_nbr = ph.pkt_ctrl_nbr
           JOIN carton_dtl cd
               ON     cd.carton_nbr = ch.carton_nbr
                  AND cd.pkt_seq_nbr = pd.pkt_seq_nbr
                  AND cd.sku_id = im.sku_id
     WHERE     im.cd_master_id IN ('18005',
                                   '10004',
                                   '18004',
                                   '11004',
                                   '6003',
                                   '4003',
                                   '5004',
                                   '9004',
                                   '9005',
                                   '9006',
                                   '5003',
                                   '12004',
                                   '17004',
                                   '13004',
                                   '15004',
                                   '3001',
                                   '3003',
                                   '10003')
           AND pi.stat_code > '39'
           AND pi.stat_code < '91'
    UNION
    SELECT DISTINCT
           CASE
               WHEN im.cd_master_id = '18005' THEN '00027'
               WHEN im.cd_master_id = '18004' THEN '00026'
               WHEN im.cd_master_id = '10004' THEN '00017'
               WHEN im.cd_master_id = '11004' THEN '00018'
               WHEN im.cd_master_id = '6003' THEN '00009'
               WHEN im.cd_master_id = '4003' THEN '00005'
               WHEN im.cd_master_id = '5004' THEN '00008'
               WHEN im.cd_master_id = '9004' THEN '00013'
               WHEN im.cd_master_id = '9005' THEN '00014'
               WHEN im.cd_master_id = '9006' THEN '00015'
               WHEN im.cd_master_id = '5003' THEN '00007'
               WHEN im.cd_master_id = '12004' THEN '00020'
               WHEN im.cd_master_id = '17004' THEN '00025'
               WHEN im.cd_master_id = '13004' THEN '00021'
               WHEN im.cd_master_id = '15004' THEN '00023'
               WHEN im.cd_master_id = '3001' THEN '00003'
               WHEN im.cd_master_id = '3003' THEN '00004'
               WHEN im.cd_master_id = '10003' THEN '00016'
               ELSE ' '
           END                                  "client_id",
           CASE
               WHEN im.cd_master_id = '18005'
               THEN
                   'ORBICO'
               WHEN im.cd_master_id = '18004'
               THEN
                   'AUTOTECH'
               WHEN im.cd_master_id = '10004'
               THEN
                   'Грейс ПРО'
               WHEN im.cd_master_id = '11004'
               THEN
                   'Karma D'
               WHEN im.cd_master_id = '6003'
               THEN
                   'Protoria'
               WHEN im.cd_master_id = '4003'
               THEN
                   'БНС Трэйд'
               WHEN im.cd_master_id = '5004'
               THEN
                   'Fors'
               WHEN im.cd_master_id = '9004'
               THEN
                   'Sialed Air'
               WHEN im.cd_master_id = '9005'
               THEN
                   'TEKA'
               WHEN im.cd_master_id = '9006'
               THEN
                   'Legrand'
               WHEN im.cd_master_id = '5003'
               THEN
                   'ДЕЙЛІ ЧОЙС'
               WHEN im.cd_master_id = '12004'
               THEN
                   'БРЕНД СТАЙЛ'
               WHEN im.cd_master_id = '17004'
               THEN
                   'БіоЛайн'
               WHEN im.cd_master_id = '13004'
               THEN
                   'Коттон'
               WHEN im.cd_master_id = '15004'
               THEN
                   'Смартмакс'
               WHEN im.cd_master_id = '3001'
               THEN
                   'MIX'
               WHEN im.cd_master_id = '3003'
               THEN
                   'Київмедпрепарат'
               WHEN im.cd_master_id = '10003'
               THEN
                   'UPECO'
               ELSE
                   ' '
           END                                  "client_name",
           pi.stat_code,
           ph.pkt_ctrl_nbr                      AS unique_id,
           NVL (ph.pro_nbr, ph.pkt_ctrl_nbr)    AS pkt_nbr,
           ph.terms_code,
           ph.shipto_addr_3                     AS TTN,
           ph.shipto,
           ph.shipto_city,
           ph.shipto_zip,
           ph.shipto_name,
           ph.shipto_addr_1,
           ph.shipto_addr_2,
           ph.shipto_contact,
           ph.soldto,
           ph.soldto_name,
           ph.soldto_addr_1,
           pd.pkt_seq_nbr,
           im.size_desc,
           im.vendor_item_nbr,
           pd.orig_pkt_qty,
           pd.orig_pkt_qty,
           im.sku_brcd,
           pd.prod_stat,
           pd.batch_nbr,
           im.sku_desc,
           pd.price,
           pd.retail_price,
           pd.unit_monetary_value               AS unit_price,
           im.unit_ht,
           im.unit_len,
           im.unit_width,
           im.unit_wt,
           NULL,
           pi.mod_date_time,
           ph.plan_load_nbr                     AS load_nbr      -- 11.10.2021
      FROM pkt_hdr  ph
           JOIN pkt_dtl pd ON pd.pkt_ctrl_nbr = ph.pkt_ctrl_nbr
           JOIN pkt_hdr_intrnl pi ON pi.pkt_ctrl_nbr = ph.pkt_ctrl_nbr
           JOIN item_master im ON im.sku_id = pd.sku_id
     WHERE     im.cd_master_id IN ('18005',
                                   '10004',
                                   '18004',
                                   '11004',
                                   '6003',
                                   '4003',
                                   '5004',
                                   '9004',
                                   '9005',
                                   '9006',
                                   '5003',
                                   '12004',
                                   '17004',
                                   '13004',
                                   '15004',
                                   '3001',
                                   '3003',
                                   '10003')
           AND (pi.stat_code < '40' OR pi.stat_code > '90')
    UNION ALL
      SELECT client_id,
             client_name,
             stat_code,
             unique_id,
             pkt_nbr,
             terms_code,
             TTN,
             shipto,
             shipto_city,
             shipto_zip,
             shipto_name,
             shipto_addr_1,
             shipto_addr_2,
             shipto_contact,
             soldto,
             soldto_name,
             soldto_addr_1,
             ROW_NUMBER ()
                 OVER (PARTITION BY unique_id ORDER BY unique_id, sku_client)
                 num,
             sku_wms,
             sku_client,
             SUM (orig_pkt_qty),
             SUM (units_pakd),
             sku_brcd,
             prod_stat,
             batch_nbr,                                          -- 16.02.2022
             sku_desc,
             price,
             retail_price,
             unit_price,
             unit_ht,
             unit_len,
             unit_width,
             unit_wt,
             ' ',
             mod_date_time,
             load_nbr                                            -- 11.10.2021
        FROM (SELECT '00029'
                         AS client_id,
                     'Вайн Хантер'
                         AS client_name,
                     pi.stat_code,
                     ph.pkt_ctrl_nbr
                         AS unique_id,
                     NVL (ph.pro_nbr, ph.pkt_ctrl_nbr)
                         AS pkt_nbr,
                     ph.terms_code,
                     ph.shipto_addr_3
                         AS TTN,
                     ph.shipto,
                     ph.shipto_city,
                     ph.shipto_zip,
                     ph.shipto_name,
                     ph.shipto_addr_1,
                     ph.shipto_addr_2,
                     ph.shipto_contact,
                     ph.soldto,
                     ph.soldto_name,
                     ph.soldto_addr_1,
                     pd.pkt_seq_nbr,
                     im.size_desc
                         AS sku_wms,
                     im.vendor_item_nbr
                         AS sku_client,
                     pd.orig_pkt_qty,
                     (SELECT SUM (ccd.units_pakd)
                        FROM carton_dtl ccd
                       WHERE     ccd.carton_nbr = cd.carton_nbr
                             AND ccd.pkt_seq_nbr = cd.pkt_seq_nbr)
                         AS units_pakd,
                     im.sku_brcd,
                     pd.prod_stat,
                     NVL (pd.batch_nbr, '*')
                         batch_nbr,                              -- 16.02.2022
                     im.sku_desc,
                     pd.price,
                     pd.retail_price,
                     pd.unit_monetary_value
                         AS unit_price,
                     im.unit_ht,
                     im.unit_len,
                     im.unit_width,
                     im.unit_wt,
                     ch.carton_nbr,
                     pi.mod_date_time,
                     ph.plan_load_nbr
                         AS load_nbr                             -- 11.10.2021
                FROM pkt_hdr ph
                     JOIN pkt_dtl pd ON pd.pkt_ctrl_nbr = ph.pkt_ctrl_nbr
                     JOIN pkt_hdr_intrnl pi
                         ON pi.pkt_ctrl_nbr = ph.pkt_ctrl_nbr
                     JOIN item_master im ON im.sku_id = pd.sku_id
                     JOIN carton_hdr ch ON ch.pkt_ctrl_nbr = ph.pkt_ctrl_nbr
                     JOIN carton_dtl cd
                         ON     cd.carton_nbr = ch.carton_nbr
                            AND cd.pkt_seq_nbr = pd.pkt_seq_nbr
               WHERE im.cd_master_id = '20004' AND pi.stat_code > '39')
    GROUP BY client_id,
             client_name,
             stat_code,
             unique_id,
             pkt_nbr,
             terms_code,
             TTN,
             shipto,
             shipto_city,
             shipto_zip,
             shipto_name,
             shipto_addr_1,
             shipto_addr_2,
             shipto_contact,
             soldto,
             soldto_name,
             soldto_addr_1,
             sku_wms,
             sku_client,
             sku_brcd,
             sku_desc,
             price,
             retail_price,
             unit_price,
             unit_ht,
             unit_len,
             unit_width,
             unit_wt,
             mod_date_time,
             prod_stat,
             load_nbr                                             --11.10.2021
                     ,
             batch_nbr                                           -- 16.02.2022
;