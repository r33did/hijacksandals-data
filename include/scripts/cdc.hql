SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.reducers.max=1;
SET mapreduce.job.reduces=1;
WITH latest_entity AS (
    SELECT
        lac_dec AS lac,
        ci_dec  AS ci,
        sha2(
            concat_ws(
                '||',
                sort_array(
                    collect_set(
                        concat_ws(
                            ',',
                            CAST(CAST(longitude AS DECIMAL(10,5)) AS STRING),
                            CAST(CAST(latitude  AS DECIMAL(10,5)) AS STRING),
                            technology,
                            bts_status,
                            tech_region,
                            area_name_sub_district,
                            city
                        )
                    )
                )
            ),
            256
        ) AS entity_hash
    FROM stg.data_lacci_com
    WHERE dt_id = '${hivevar:dt_id}'
    GROUP BY lac_dec, ci_dec
),

prev_entity AS (
    SELECT
        lac_dec AS lac,
        ci_dec  AS ci,
        sha2(
            concat_ws(
                '||',
                sort_array(
                    collect_set(
                        concat_ws(
                            ',',
                            CAST(CAST(longitude AS DECIMAL(10,5)) AS STRING),
                            CAST(CAST(latitude  AS DECIMAL(10,5)) AS STRING),
                            technology,
                            bts_status,
                            tech_region,
                            area_name_sub_district,
                            city
                        )
                    )
                )
            ),
            256
        ) AS entity_hash
    FROM stg.data_lacci_com
    WHERE dt_id = '${hivevar:priordt_id}'
    GROUP BY lac_dec, ci_dec
),
changed_entities AS (
    SELECT
        l.lac,
        l.ci
    FROM latest_entity l
    LEFT JOIN prev_entity p
        ON l.lac = p.lac
       AND l.ci  = p.ci
    WHERE p.lac IS NULL
       OR l.entity_hash <> p.entity_hash
),
cdc_changes AS (
    SELECT
        t.lac_dec AS lac,
        t.ci_dec  AS ci,
        CAST(t.longitude AS DECIMAL(10,5)) AS `long`,
        CAST(t.latitude  AS DECIMAL(10,5)) AS lat,
        t.technology AS rattype,
        t.bts_status AS `status`,
        t.tech_region AS region,
        t.area_name_sub_district AS subdistrict,
        t.city AS district,
        0 AS is_deleted,
        '${hivevar:dt_id}' AS dt_id
    FROM stg.data_lacci_com t
    JOIN changed_entities c
      ON t.lac_dec = c.lac
     AND t.ci_dec  = c.ci
    WHERE t.dt_id = '${hivevar:dt_id}'
),
cdc_deletes AS (
    SELECT
        p.lac,
        p.ci,
        NULL AS `long`,
        NULL AS lat,
        NULL AS rattype,
        NULL AS `status`,
        NULL AS region,
        NULL AS subdistrict,
        NULL AS district,
        1 AS is_deleted,
        '${hivevar:dt_id}' AS dt_id
    FROM prev_entity p
    LEFT JOIN latest_entity l
      ON p.lac = l.lac
     AND p.ci  = l.ci
    WHERE l.lac IS NULL
),
cdc_staging as (
    select *,0 is_sent, sha2(concat_ws('||',
            CAST(lac AS STRING),
            CAST(ci AS STRING),
            COALESCE(CAST(`long` AS STRING), 'NULL'),
            COALESCE(CAST(lat AS STRING), 'NULL'),
            COALESCE(rattype, 'NULL'),
            COALESCE(`status`, 'NULL'),
            COALESCE(region, 'NULL'),
            COALESCE(subdistrict, 'NULL'),
            COALESCE(district, 'NULL'),
            CAST(is_deleted AS STRING)
        ), 256) AS row_hash
    from
    (SELECT lac, ci,
            CAST(`long` AS STRING) AS `long`,
            CAST(lat AS STRING) AS lat,
            rattype, `status`, region, subdistrict, district, is_deleted, dt_id 
    FROM cdc_changes
    UNION ALL
    SELECT 
        lac, ci,
        CAST(`long` AS STRING) AS `long`,
        CAST(lat AS STRING) AS lat,
        rattype, `status`, region, subdistrict, district, is_deleted, dt_id 
    FROM cdc_deletes) a
),
existing_hashes AS (
    SELECT sha2(concat_ws('||',
            CAST(lac AS STRING),
            CAST(ci AS STRING),
            COALESCE(CAST(`long` AS STRING), 'NULL'),
            COALESCE(CAST(lat AS STRING), 'NULL'),
            COALESCE(rattype, 'NULL'),
            COALESCE(`status`, 'NULL'),
            COALESCE(region, 'NULL'),
            COALESCE(subdistrict, 'NULL'),
            COALESCE(district, 'NULL'),
            CAST(is_deleted AS STRING)
        ), 256) AS row_hash
    FROM stg.ldm_cdc_lacci_com
    WHERE dt_id = '${hivevar:dt_id}'
)
INSERT OVERWRITE TABLE stg.ldm_cdc_lacci_com_staging PARTITION (dt_id)
SELECT lac, ci, `long`, lat, rattype, `status`, region, subdistrict, district, is_deleted, is_sent, dt_id
FROM cdc_staging s
WHERE NOT EXISTS (
    SELECT 1 FROM existing_hashes e WHERE e.row_hash = s.row_hash
)
UNION ALL
SELECT lac, ci, `long`, lat, rattype, `status`, region, subdistrict, district, is_deleted, COALESCE(is_sent, 0) is_sent, dt_id
FROM stg.ldm_cdc_lacci_com
WHERE dt_id = '${hivevar:dt_id}';

INSERT OVERWRITE TABLE stg.ldm_cdc_lacci_com PARTITION (dt_id)
SELECT lac, ci, `long`, lat, rattype, `status`, region, subdistrict, district, is_deleted, COALESCE(is_sent, 0) is_sent, dt_id
FROM stg.ldm_cdc_lacci_com_staging
WHERE dt_id = '${hivevar:dt_id}';

ALTER TABLE stg.ldm_cdc_lacci_com_staging DROP IF EXISTS PARTITION (dt_id='${hivevar:dt_id}');