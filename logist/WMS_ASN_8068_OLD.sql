CREATE OR REPLACE VIEW WMS_ASN_8068_OLD AS
SELECT client_id,
             client_name,
             stat_code,
             unique_id,
             shpmt_nbr,
             shpmt_seq_nbr,
             sku_wms,
             sku_client,
             sku_brcd,
             sku_desc,
             prod_stat,
             batch_nbr,
             units_shpd,
             units_rcvd,
             mod_date_time
        FROM (SELECT CASE
                         WHEN im.cd_master_id = '18005' THEN '00027'
                         WHEN im.cd_master_id = '10004' THEN '00017'
                         WHEN im.cd_master_id = '10003' THEN '00016'
                         WHEN im.cd_master_id = '18004' THEN '00026'
                         WHEN im.cd_master_id = '11004' THEN '00018'
                         WHEN im.cd_master_id = '6003' THEN '00009'
                         WHEN im.cd_master_id = '4003' THEN '00005'
                         ELSE ' '
                     END                                      AS client_id,
                     CASE
                         WHEN im.cd_master_id = '18005'
                         THEN
                             'ORBICO'
                         WHEN im.cd_master_id = '10004'
                         THEN
                             'Грейс ПРО'
                         WHEN im.cd_master_id = '10003'
                         THEN
                             'Upeco'
                         WHEN im.cd_master_id = '18004'
                         THEN
                             'AUTOTECH'
                         WHEN im.cd_master_id = '11004'
                         THEN
                             'Карма Диджитал'
                         WHEN im.cd_master_id = '6003'
                         THEN
                             'Protoria'
                         WHEN im.cd_master_id = '4003'
                         THEN
                             'БНС Трэйд'
                         ELSE
                             ' '
                     END                                      AS client_name,
                     ah.stat_code,
                     ah.shpmt_nbr                             AS unique_id,
                     DECODE (im.cd_master_id,
                             '10003', SUBSTR (ah.shpmt_nbr, 2, 20),
                             SUBSTR (ah.shpmt_nbr, 3, 20))    AS shpmt_nbr,
                     ad.shpmt_seq_nbr,
                     im.size_desc                             sku_wms,
                     im.vendor_item_nbr                       sku_client,
                     im.sku_brcd,
                     im.sku_desc,
                     ad.prod_stat,
                     ad.batch_nbr,
                     ad.units_shpd,
                     ad.units_rcvd,
                     ah.mod_date_time
                FROM asn_hdr ah
                     JOIN asn_dtl ad ON ad.shpmt_nbr = ah.shpmt_nbr
                     JOIN item_master im ON im.sku_id = ad.sku_id
               WHERE im.cd_master_id IN ('18005',
                                         '10004',
                                         '10003',
                                         '18004',
                                         '11004',
                                         '6003',
                                         '4003')
              UNION ALL
                SELECT client_id,
                       client_name,
                       stat_code,
                       unique_id,
                       shpmt_nbr,
                       ROW_NUMBER ()
                           OVER (PARTITION BY unique_id
                                 ORDER BY unique_id, sku_wms)    num,
                       sku_wms,
                       sku_client,
                       sku_brcd,
                       sku_desc,
                       prod_stat,
                       NVL (batch_nbr, '*')                      batch_nbr, -- 16.02.2022
                       SUM (units_shpd)                          units_shpd,
                       SUM (units_rcvd)                          units_rcvd,
                       mod_date_time
                  FROM (SELECT '00029'                     client_id,
                               'Вайн Хантер'     client_name,
                               ah.stat_code,
                               ah.shpmt_nbr                AS unique_id,
                               ah.shpmt_nbr,
                               ad.shpmt_seq_nbr,
                               im.size_desc                sku_wms,
                               im.vendor_item_nbr          sku_client,
                               im.sku_brcd,
                               im.sku_desc,
                               ad.prod_stat,
                               ad.batch_nbr,
                               ad.units_shpd,
                               ad.units_rcvd,
                               ah.mod_date_time
                          FROM asn_hdr ah
                               JOIN asn_dtl ad ON ad.shpmt_nbr = ah.shpmt_nbr
                               JOIN item_master im ON im.sku_id = ad.sku_id
                         WHERE im.cd_master_id = '20004' AND ad.units_rcvd > 0 -- 16.02.2022
                                                                              )
              GROUP BY client_id,
                       client_name,
                       stat_code,
                       unique_id,
                       shpmt_nbr,
                       sku_wms,
                       sku_client,
                       sku_brcd,
                       sku_desc,
                       mod_date_time,
                       prod_stat,
                       batch_nbr                                 -- 16.02.2022
              ORDER BY 4, 8)
    ORDER BY 5, 6
;