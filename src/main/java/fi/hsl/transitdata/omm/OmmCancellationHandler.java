package fi.hsl.transitdata.omm;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

public class OmmCancellationHandler {
    static final Logger log = LoggerFactory.getLogger(OmmCancellationHandler.class);

    String timeZone;
    private final Producer<byte[]> producer;

    public OmmCancellationHandler(PulsarApplicationContext context) {
        producer = context.getProducer();
        timeZone = context.getConfig().getString("omm.timezone");
    }

    //TODO Move to commons
    public Optional<Long> toUtcEpochMs(String localTimestamp) {
        return toUtcEpochMs(localTimestamp, timeZone);
    }

    public static Optional<Long> toUtcEpochMs(String localTimestamp, String zoneId) {
        if (localTimestamp == null || localTimestamp.isEmpty())
            return Optional.empty();

        try {
            LocalDateTime dt = LocalDateTime.parse(localTimestamp.replace(" ", "T")); // Make java.sql.Timestamp ISO compatible
            ZoneId zone = ZoneId.of(zoneId);
            long epochMs = dt.atZone(zone).toInstant().toEpochMilli();
            return Optional.of(epochMs);
        }
        catch (Exception e) {
            log.error("Failed to parse datetime from " + localTimestamp, e);
            return Optional.empty();
        }
    }


    public void handleAndSend(ResultSet resultSet) throws SQLException, PulsarClientException {
        while (resultSet.next()) {

            InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();

            String routeId = resultSet.getString("ROUTE_NAME");
            builder.setRouteId(routeId);
            int joreDirection = resultSet.getInt("DIRECTION");
            builder.setDirectionId(joreDirection);
            String startDate = resultSet.getString("OPERATING_DAY"); // yyyyMMdd
            builder.setStartDate(startDate);
            String starTime = resultSet.getString("START_TIME"); // HH:mm:ss in local time
            builder.setStartTime(starTime);

            //Version number is defined in the proto file as default value but we still need to set it since it's a required field
            builder.setSchemaVersion(builder.getSchemaVersion());
            builder.setStatus(InternalMessages.TripCancellation.Status.CANCELED);

            final InternalMessages.TripCancellation cancellation = builder.build();

            final String dvjId = Long.toString(resultSet.getLong("DVJ_ID"));
            final String description = resultSet.getString("description");
            log.debug("Read cancellation for route {} with  dvjId {} and description '{}'",
                    routeId, dvjId, description);

            Timestamp timestamp = resultSet.getTimestamp("affected_departure_last_modified"); //other option is to use dev_case_last_modified
            Optional<Long> epochTimestamp = toUtcEpochMs(timestamp.toString(), timeZone);
            if (!epochTimestamp.isPresent()) {
                log.error("Failed to parse epoch timestamp from resultset: {}", timestamp.toString());
            }
            else {
                sendPulsarMessage(cancellation, epochTimestamp.get(), dvjId);
            }
        }
    }

    private void sendPulsarMessage(InternalMessages.TripCancellation tripCancellation, long timestamp, String dvjId) throws PulsarClientException {
        try {
            producer.newMessage().value(tripCancellation.toByteArray())
                    .eventTime(timestamp)
                    .key(dvjId)
                    .property(TransitdataProperties.KEY_DVJ_ID, dvjId)
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation.toString())
                    .send();

            log.info("Produced a cancellation for trip: " + tripCancellation.getRouteId() + "/" +
                    tripCancellation.getDirectionId() + "-" + tripCancellation.getStartTime() + "-" +
                    tripCancellation.getStartDate());

        }
        catch (PulsarClientException pe) {
            log.error("Failed to send message to Pulsar", pe);
            throw pe;
        }
        catch (Exception e) {
            log.error("Failed to handle cancellation message", e);
        }
    }

}
