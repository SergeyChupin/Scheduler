package com.pixonic.scheduler.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;

public class LocalDateTimeUtils {

    public static long toMillis(LocalDateTime dateTime, ZoneId zoneId) {
        Objects.requireNonNull(dateTime, "DateTime should be specified");
        Objects.requireNonNull(zoneId, "ZoneId should be specified");
        return dateTime.atZone(zoneId).toInstant().toEpochMilli();
    }

    public static LocalDateTime fromMillis(long millis, ZoneId zoneId) {
        Objects.requireNonNull(zoneId, "ZoneId should be specified");
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), zoneId);
    }
}
