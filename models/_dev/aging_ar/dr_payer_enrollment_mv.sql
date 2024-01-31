{{ config(
    materialized = 'materialized_view',
    sort = ['doctor_id'],
)}}

SELECT spe.id,
        spe.doctor_id,
        spe.payer_id,
        spe.payer_name,
        spe.payer_grouping
FROM   {{source('chronometer_production','chronometer_simpledrpayerenrollment')}} spe
WHERE  spe.payer_grouping::text <> ''::text
