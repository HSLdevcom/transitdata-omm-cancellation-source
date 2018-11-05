package fi.hsl.transitdata.omm;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

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

            String routeId = resultSet.getString(1);//TODO FIX INDEX AND DATA TYPE
            builder.setRouteId(routeId);
            int direction = resultSet.getInt(2);//TODO FIX INDEX AND DATA TYPE
            builder.setDirectionId(direction);
            String startDate = resultSet.getString(3);//TODO FIX INDEX AND DATA TYPE
            builder.setStartDate(startDate);
            String starTime = resultSet.getString(4);//TODO FIX INDEX AND DATA TYPE
            builder.setStartTime(starTime);

            //Version number is defined in the proto file as default value but we still need to set it since it's a required field
            builder.setSchemaVersion(builder.getSchemaVersion());
            builder.setStatus(InternalMessages.TripCancellation.Status.CANCELED);

            InternalMessages.TripCancellation cancellation = builder.build();

            final long timestamp = resultSet.getLong(5); //TODO FIX INDEX AND DATA TYPE
            String dvjId ="";//TODO get from somewhere

            sendPulsarMessage(cancellation, timestamp, dvjId);
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
