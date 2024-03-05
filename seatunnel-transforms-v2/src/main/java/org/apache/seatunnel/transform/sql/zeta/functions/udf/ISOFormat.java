package org.apache.seatunnel.transform.sql.zeta.functions.udf;

import com.google.auto.service.AutoService;
import com.google.common.base.Strings;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.List;

@AutoService(ZetaUDF.class)
public class ISOFormat implements ZetaUDF {
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    public String functionName() {
        return "ISO_FORMAT";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String dateStr = (String) args.get(0);
        String NewFormat = (String) args.get(1);
        if (dateStr == null) {return null;}

        DateTimeFormatter parser1 = DateTimeFormatter.ISO_DATE_TIME;

        // Create a new formatter for the non-colon time zone format
        DateTimeFormatter parser2 = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
                .toFormatter();

        DateTimeFormatter formatter = Strings.isNullOrEmpty(NewFormat) ?
                DateTimeFormatter.ofPattern(DEFAULT_FORMAT) : DateTimeFormatter.ofPattern(NewFormat);
        try {
            ZonedDateTime date = ZonedDateTime.parse(dateStr, parser1);
            return date.format(formatter).toString();
        } catch (Exception e1) {
            try {
                ZonedDateTime date = ZonedDateTime.parse(dateStr, parser2);
                return date.format(formatter).toString();
            } catch (Exception e2) {
                e2.printStackTrace();
                return null;
            }
        }
    }

    public Object evaluate2(List<Object> args) {
        String dateStr = (String) args.get(0);
        String NewFormat = (String) args.get(1);
        if (dateStr == null) {return null;}

        DateTimeFormatter dtfInput = new DateTimeFormatterBuilder()
                .appendPattern("[yyyy-MM-dd'T'HH:mm:ss.SSSX][yyyy-MM-dd'T'HH:mm:ssX][yyyy-MM-dd'T'HH:mm:ssXXX]")
                .toFormatter();
        DateTimeFormatter formatter = Strings.isNullOrEmpty(NewFormat) ?
                DateTimeFormatter.ofPattern(DEFAULT_FORMAT) : DateTimeFormatter.ofPattern(NewFormat);
        try {
            TemporalAccessor ta = dtfInput.parseBest(dateStr, ZonedDateTime::from, LocalDateTime::from, LocalDate::from);
            if (ta instanceof ZonedDateTime){
                ZonedDateTime zdt = ZonedDateTime.from(ta);
                return zdt.format(formatter).toString();
            } else if (ta instanceof LocalDateTime){
                LocalDateTime ldt = LocalDateTime.from(ta);
                return ldt.format(formatter).toString();
            } else if (ta instanceof LocalDate){
                LocalDate ld = LocalDate.from(ta);
                return ld.format(formatter).toString();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }
}
