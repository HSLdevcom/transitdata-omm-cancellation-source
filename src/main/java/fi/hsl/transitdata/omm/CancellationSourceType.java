package fi.hsl.transitdata.omm;

public enum CancellationSourceType {
    FROM_HISTORY,
    FROM_NOW;

    public String toString() {
        switch (this) {
            case FROM_HISTORY: return "fromHistory";
            case FROM_NOW: return "fromNow";
        }
        return "";
    }

    public static CancellationSourceType fromString(String cancellationSourceType) {
        if ("HISTORY".equals(cancellationSourceType))
            return FROM_HISTORY;
        else if ("NOW".equals(cancellationSourceType))
            return FROM_NOW;
        return null;
    }
}
