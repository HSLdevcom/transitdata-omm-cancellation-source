package fi.hsl.transitdata.omm;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class OmmConnector {

    private static final Logger log = LoggerFactory.getLogger(OmmConnector.class);

    private final Connection dbConnection;
    private OmmCancellationHandler handler;
    private final String queryString;
    private final String timezone;

    private OmmConnector(PulsarApplicationContext context, Connection connection) {
        handler = new OmmCancellationHandler(context);
        dbConnection = connection;
        queryString = createQuery();
        timezone = context.getConfig().getString("omm.timezone");
        log.info("Using timezone " + timezone);
    }

    public static OmmConnector newInstance(PulsarApplicationContext context, String jdbcConnectionString) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new OmmConnector(context, connection);
    }

    private String createQuery() {
        return "SELECT " +
                "      DC.[valid_from] AS VALID_FROM" +
                "      ,DC.[valid_to] AS VALID_TO" +
                "      ,DC.[type] AS DEVIATION_CASES_TYPE" +
                "      ,DC.[last_modified] AS DEVIATION_CASES_LAST_MODIFIED" +
                "      ,AD.last_modified AS AFFECTED_DEPARTURES_LAST_MODIFIED" +
                "      ,AD.[status] AS AFFECTED_DEPARTURES_STATUS " +
                "      ,AD.[type] AS AFFECTED_DEPARTURES_TYPE" +
                "      ,BLM.[title] AS TITLE" +
                "      ,BLM.[description] AS DESCRIPTION" +
                "      ,B.category AS CATEGORY" +
                "      ,B.sub_category as SUB_CATEGORY" +
                "      ,CONVERT(CHAR(16), DVJ.Id) AS DVJ_ID, KVV.StringValue AS ROUTE_NAME" +
                "      ,CONVERT(INTEGER, SUBSTRING(CONVERT(CHAR(16), VJT.IsWorkedOnDirectionOfLineGid), 12, 1)) AS DIRECTION" +
                "      ,CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS OPERATING_DAY, " +
                "           RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime)))), 2) + ':' + " +
                "           RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', PlannedStartOffsetDateTime))- " +
                "                ((DATEDIFF(HOUR, '1900-01-01', PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS START_TIME " +
                "  FROM [OMM_Community].[dbo].[deviation_cases] AS DC" +
                "  LEFT JOIN OMM_Community.dbo.affected_departures AS AD ON DC.deviation_case_id = AD.deviation_case_id" +
                "  LEFT JOIN OMM_Community.dbo.bulletin_localized_messages AS BLM ON DC.bulletin_id = BLM.bulletins_id" +
                "  LEFT JOIN OMM_Community.dbo.bulletins AS B ON DC.bulletin_id = B.bulletins_id" +
                "  INNER JOIN ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ ON DVJ.Id = AD.departure_id" +
                "  INNER JOIN ptDOI4_Community.dbo.VehicleJourney AS VJ ON VJ.Id = DVJ.IsBasedOnVehicleJourneyId" +
                "  INNER JOIN ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT ON VJT.Id = DVJ.IsBasedOnVehicleJourneyTemplateId" +
                "  INNER JOIN ptDOI4_Community.T.KeyVariantValue AS KVV ON KVV.IsForObjectId = VJ.Id" +
                "  INNER JOIN ptDOI4_Community.dbo.KeyVariantType AS KVT ON KVT.Id = KVV.IsOfKeyVariantTypeId" +
                "  INNER JOIN ptDOI4_Community.dbo.KeyType AS KT ON KT.Id = KVT.IsForKeyTypeId" +
                "  INNER JOIN ptDOI4_Community.dbo.ObjectType AS OT ON OT.Number = KT.ExtendsObjectTypeNumber" +
                "  WHERE DC.[type] = 'CANCEL_DEPARTURE' AND AD.[type] = 'CANCEL_ENTIRE_DEPARTURE'" +
                "  AND BLM.language_code = 'fi'" +
                "  AND " +
                "       (DC.valid_to > ?" +
                // workaround to get deleted cancellations, because valid_to-field is nulled for these ones.
                "           OR (DC.valid_to IS NULL AND AD.[status] = 'deleted' AND DVJ.OperatingDayDate >= ?))" +
                "  AND (KT.Name = 'JoreIdentity' OR KT.Name = 'JoreRouteIdentity' OR KT.Name = 'RouteName') AND OT.Name = 'VehicleJourney'" +
                "  AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL" +
                "  AND DVJ.IsReplacedById IS NULL" +
                "  ORDER BY DC.last_modified;";
    }

    static String localDatetimeAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(instant.atZone(ZoneId.of(zoneId)));
    }

    static String localDateAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(instant.atZone(ZoneId.of(zoneId)));
    }

    public void queryAndProcessResults() throws SQLException, PulsarClientException {
        //Let's use Strings in the query since JDBC driver tends to convert timestamps automatically to local jvm time.
        Instant now = Instant.now();
        String nowDateTime = localDatetimeAsString(now, timezone);
        String nowDate = localDateAsString(now, timezone);

        log.info("Querying results from database with timestamp {}", now);
        long queryStartTime = System.currentTimeMillis();

        log.trace("Running query " + queryString);

        try (PreparedStatement statement = dbConnection.prepareStatement(queryString)) {
            statement.setString(1, nowDateTime);
            statement.setString(2, nowDate);

            ResultSet resultSet = statement.executeQuery();
            handler.handleAndSend(resultSet);

            long elapsed = System.currentTimeMillis() - queryStartTime;
            log.info("Messages handled. Total query and processing time: {} ms", elapsed);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

}
