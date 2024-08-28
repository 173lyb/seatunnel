package org.apache.seatunnel.transform.sql.zeta.functions.udf;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** @Author 熊呈 @Mail xiongcheng@xugudb.com @Date 2024/8/9 15:17 */
@AutoService(ZetaUDF.class)
public class TimeTrans implements ZetaUDF {
    @Override
    public String functionName() {
        return "TimeTrans";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> list) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> list) {
        String timeValue = list.get(0).toString();
        if (StringUtils.isBlank(timeValue)) {
            return null;
        }
        timeValue = timeValue.replace("T", " ");
        String format = list.get(1).toString();
        try {
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern(format);
            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            LocalDateTime dateTime = LocalDateTime.parse(timeValue, inputFormatter);
            return dateTime.format(outputFormatter);
        } catch (Exception e) {
            throw new RuntimeException("转换日期失败" + e);
        }
    }
}
