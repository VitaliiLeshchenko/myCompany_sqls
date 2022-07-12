CREATE OR REPLACE VIEW LOGIST.WMS_ASN_8068 AS
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
        FROM (SELECT fn_cd_master_id_tocode_8068(im.cd_master_id)  AS client_id,
                     fn_cd_master_id_toDesc_8068(im.cd_master_id)  AS client_name,
                     ah.stat_code                                  AS stat_code,
                     ah.shpmt_nbr                                  AS unique_id,
                     DECODE (im.cd_master_id,
                             '10003', SUBSTR (ah.shpmt_nbr, 2, 20),
                             SUBSTR (ah.shpmt_nbr, 3, 20))         AS shpmt_nbr,
                     ad.shpmt_seq_nbr,
                     im.size_desc                                  AS sku_wms,
                     im.vendor_item_nbr                            AS sku_client,
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
               WHERE im.cd_master_id IN ('3001', --16.06.2022
                                         '3003',
                                         '4003',
                                         '5003',
                                         '5004',
                                         '6003',
                                         '9004',
                                         '9005',
                                         '9006',
                                         '10003',
                                         '10004',
                                         '11004',
                                         '11005',  --12.07.2022
                                         '12004',
                                         '13004',
                                         '15004',
                                         '17004',
                                         '18004',
                                         '18005')
              UNION ALL
                SELECT client_id,
                       client_name,
                       stat_code,
                       unique_id,
                       shpmt_nbr,
                       shpmt_seq_nbr,  --12.07.2022
                       sku_wms,
                       sku_client,
                       sku_brcd,
                       sku_desc,
                       prod_stat,
                       NVL (batch_nbr, '*')                      batch_nbr, -- 16.02.2022
                       SUM (units_shpd)                          units_shpd,
                       SUM (units_rcvd)                          units_rcvd,
                       mod_date_time
                  FROM (SELECT '00029'                     AS client_id,
                               'Вайн Хантер'               AS client_name,
                               ah.stat_code,
                               ah.shpmt_nbr                AS unique_id,
                               ah.shpmt_nbr,
                               ad.shpmt_seq_nbr,
                               im.size_desc                AS sku_wms,
                               im.vendor_item_nbr          AS sku_client,
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
                       shpmt_seq_nbr,  --12.07.2022
                       sku_client,
                       sku_brcd,
                       sku_desc,
                       mod_date_time,
                       prod_stat,
                       batch_nbr                                 -- 16.02.2022
              ORDER BY 4, 8)
    ORDER BY 5, 6;