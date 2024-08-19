package org.apache.seatunnel.connectors.seatunnel.http;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MyDateUtil {

    private static final DateTimeFormatter sdfSecond =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Gets the date string for a given day offset from today.
     *
     * @param dayOffset The number of days to offset from today. Negative values represent days in
     *     the past.
     * @return A string representing the date in "yyyy-MM-dd HH:mm:ss" format.
     */
    public static String getDateStrByDayOffset2(int dayOffset) {
        LocalDateTime dateTime = LocalDateTime.now().minusDays(Math.abs(dayOffset));
        return sdfSecond.format(dateTime);
    }

    public static void main(String[] args) {
        String dateStr = getDateStrByDayOffset2(-30);
        System.out.println(dateStr); // Example output: 2023-09-01 00:00:00 (if today is 2023-10-01)
    }
}
