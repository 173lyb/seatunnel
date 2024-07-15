/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.http.util;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class TimeUtils {
    private static final Map<Formatter, DateTimeFormatter> FORMATTER_MAP =
            new HashMap<Formatter, DateTimeFormatter>();

    static {
        FORMATTER_MAP.put(
                Formatter.HH_MM_SS, DateTimeFormatter.ofPattern(Formatter.HH_MM_SS.value));
        FORMATTER_MAP.put(
                Formatter.HH_MM_SS_SSS, DateTimeFormatter.ofPattern(Formatter.HH_MM_SS_SSS.value));
    }

    public static LocalDateTime parseDateTime(String dateTime, Formatter formatter) {
        return LocalDateTime.parse(dateTime, DateTimeFormatter.ofPattern(formatter.getValue()));
    }

    public static long toEpochSecond(LocalDateTime dateTime) {
        return dateTime.atZone(ZoneId.of("Asia/Shanghai")).toEpochSecond();
    }

    public static long toEpochMill(LocalDateTime dateTime) {
        return dateTime.atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();
    }

    public static LocalTime parse(String time, Formatter formatter) {
        return LocalTime.parse(time, FORMATTER_MAP.get(formatter));
    }

    public static String toString(LocalTime time, Formatter formatter) {
        return time.format(FORMATTER_MAP.get(formatter));
    }

    public enum Formatter {
        YYYY_MM_DD("yyyy-MM-dd"),
        HH_MM_SS("HH:mm:ss"),
        HH_MM_SS_SSS("HH:mm:ss.SSS"),
        YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SSS("yyyy-MM-dd HH:mm:ss.SSS");
        private final String value;

        Formatter(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Formatter parse(String format) {
            Formatter[] formatters = Formatter.values();
            for (Formatter formatter : formatters) {
                if (formatter.getValue().equals(format)) {
                    return formatter;
                }
            }
            String errorMsg = String.format("Illegal format [%s]", format);
            throw new IllegalArgumentException(errorMsg);
        }
    }
}
