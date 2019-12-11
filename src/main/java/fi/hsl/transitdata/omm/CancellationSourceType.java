package fi.hsl.transitdata.omm;

public enum CancellationSourceType {
    FROM_PAST,
    FROM_NOW;

    public String toString() {
        switch (this) {
            case FROM_PAST: return "fromPast";
            case FROM_NOW: return "fromNow";
        }
        return "";
    }

    public static CancellationSourceType fromString(String cancellationSourceType) {
        if ("PAST".equals(cancellationSourceType))
            return FROM_PAST;
        else if ("NOW".equals(cancellationSourceType))
            return FROM_NOW;
        return null;
    }
}
