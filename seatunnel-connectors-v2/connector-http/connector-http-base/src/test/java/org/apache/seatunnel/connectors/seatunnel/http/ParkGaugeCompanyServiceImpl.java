package org.apache.seatunnel.connectors.seatunnel.http;

import org.apache.seatunnel.connectors.seatunnel.http.util.Md5Util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 数字孪生-规上企业信息 服务实现类
 *
 * @author liuxin
 * @since 2023-12-12
 */
public class ParkGaugeCompanyServiceImpl {

    private static final String APP_KEY = "zpWLsVcZjuhR9JPH";
    private static final String APP_SECRET = "a6de2fbe63ef781dc6547184266da7d7";
    private static final String APP_URL =
            "https://open.lqysz.com/service-open/parkBusiness/getParkGaugeCompany";

    public static void main(String[] args) {

        long timeMillis = System.currentTimeMillis();
        // 来源系统的时间比我方晚一分钟左右,故此处时间戳减去70秒
        long ts = (timeMillis / 1000) - 70;
        String signature =
                Md5Util.md5Lower(
                        "appKey=" + APP_KEY + "&appSecret=" + APP_SECRET + "&timestamp=" + ts);

        JSONObject params = new JSONObject();
        params.put("appKey", APP_KEY);
        params.put("appSecret", APP_SECRET);
        params.put("signature", signature);
        params.put("timestamp", ts);

        String resp = HttpRequest.post(APP_URL, params.toJSONString());
        System.out.println(resp);
        JSONObject respJson = JSONObject.parseObject(resp);
        JSONArray data = respJson.getJSONArray("data");
    }

    public void syncCompany() {

        //        if (!CollectionUtils.isEmpty(data)) {
        //            List<ParkGaugeCompany> companyList = data.toJavaList(ParkGaugeCompany.class);
        //            // 由于来源数据没有主键,需删除原有数据
        //            remove(null);
        //            saveBatch(companyList);
        //        }
    }
}
