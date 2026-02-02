SELECT
	DC.deviation_case_id AS deviation_case_id,
    DC.[valid_from] AS VALID_FROM
    ,DC.[valid_to] AS VALID_TO
    ,DC.[type] AS DEVIATION_CASES_TYPE
    ,DC.[last_modified] AS DEVIATION_CASES_LAST_MODIFIED
    ,AD.last_modified AS AFFECTED_DEPARTURES_LAST_MODIFIED
    ,AD.[status] AS AFFECTED_DEPARTURES_STATUS
    ,AD.[type] AS AFFECTED_DEPARTURES_TYPE
    ,BLM.[title] AS TITLE
    ,BLM.[description] AS DESCRIPTION
    ,B.category AS CATEGORY
    ,B.sub_category AS SUB_CATEGORY
    ,CONVERT(CHAR(16), DVJ.Id) AS DVJ_ID, KVV.StringValue AS ROUTE_NAME
    ,CONVERT(INTEGER, SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1)) AS DIRECTION
    ,CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS OPERATING_DAY
    ,RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) + ':' +
     RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime))- +
                ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS START_TIME
    FROM omm_db.dbo.deviation_cases AS DC
    LEFT JOIN omm_db.dbo.affected_departures AS AD ON DC.deviation_case_id = AD.deviation_case_id
    LEFT JOIN omm_db.dbo.bulletin_localized_messages AS BLM ON DC.bulletin_id = BLM.bulletins_id
    LEFT JOIN omm_db.dbo.bulletins AS B ON DC.bulletin_id = B.bulletins_id
    INNER JOIN ptDOI4.dbo.DatedVehicleJourney AS DVJ ON DVJ.Id = AD.departure_id
    INNER JOIN ptDOI4.dbo.VehicleJourney AS VJ ON VJ.Id = DVJ.IsBasedOnVehicleJourneyId
    INNER JOIN ptDOI4.dbo.VehicleJourneyTemplate AS VJT ON VJT.Id = DVJ.IsBasedOnVehicleJourneyTemplateId
    INNER JOIN ptDOI4.dbo.T_KeyVariantValue AS KVV ON KVV.IsForObjectId = VJ.Id
    INNER JOIN ptDOI4.dbo.KeyVariantType AS KVT ON KVT.Id = KVV.IsOfKeyVariantTypeId
    INNER JOIN ptDOI4.dbo.KeyType AS KT ON KT.Id = KVT.IsForKeyTypeId
    INNER JOIN ptDOI4.dbo.ObjectType AS OT ON OT.Number = KT.ExtendsObjectTypeNumber
    WHERE BLM.language_code = 'fi'
    AND ((DC.valid_to > ? OR (DC.valid_to IS NULL AND AD.[status] = 'deleted' AND DVJ.OperatingDayDate >= ?))
        OR
        ((DC.valid_to <= ? OR (DC.valid_to IS NULL AND AD.[status] = 'deleted' AND DVJ.OperatingDayDate < ?))
            AND DC.last_modified >= ?))
    AND (KT.Name = 'JoreIdentity' OR KT.Name = 'JoreRouteIdentity' OR KT.Name = 'RouteName') AND OT.Name = 'VehicleJourney'
    AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL
    AND DVJ.IsReplacedById IS NULL
    ORDER BY DC.last_modified;
