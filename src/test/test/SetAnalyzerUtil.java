package test;

import cn.sinobest.base.util.JsonUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SetAnalyzerUtil {
    private static String ANALYZER = "ik_smart";
    private static String SEARCH_ANALYZER = "ik_smart";
    private static String[] FIELDS_DW = {"DWMC", "GJ", "FRDB", "DWSZDPCS_CN", "XXDZ", "XXDZMS", "DWLX", "RESERVATION05_CN", "ZCZJDW_CN", "DEPARTMENTCODE_CN", "FADD_JD", "AJZBRY_CN", "DEPARTMENTCODE_CN", "AJLARY_CN", "LADW_CN", "SLJJDW_CN", "ZABZ", "SLJSDW_CN", "XZSJ", "SL_JJFS", "ZBDW_CN", "AJXBRY_CN", "AJSTATE", "AB", "XZCS", "ZAGJ", "AJLX", "FADY", "FADD", "ZYAQ", "AJMC", "SSSQ"};
    private static String[] FIELDS_AJ = {"FADD_JD", "AJZBRY_CN", "DEPARTMENTCODE_CN", "AJLARY_CN", "LADW_CN", "SLJJDW_CN", "ZABZ", "SLJSDW_CN", "XZSJ", "SL_JJFS", "ZBDW_CN", "AJXBRY_CN", "AJSTATE", "AB", "XZCS", "ZAGJ", "AJLX", "FADY", "FADD", "ZYAQ", "AJMC", "SSSQ"};
    private static String[] FIELDS_JQ = {"DEPARTMENTCODE_CN", "SL_LRR_CN", "SLJSDW_CN", "YSDW_CN", "SL_AJCLQK", "SL_LDQZ", "AB", "RESERVATION05_CN", "CJDW_CN", "BJDD", "BJLY", "RESERVATION11", "RESERVATION13_CN", "SLJJRY", "SLJJDW_CN", "RESERVATION10", "SL_CLJG", "JQLB", "SL_JJFS", "ZBDW_CN", "AJXBRY_CN", "FADD", "ZYAQ", "SSSQ"};
    private static String[] FIELDS_RY = {"FADD_JD", "AJZBRY_CN", "DEPARTMENTCODE_CN", "AJLARY_CN", "LADW_CN", "SLJJDW_CN", "ZABZ", "SLJSDW_CN", "XZSJ", "SL_JJFS", "ZBDW_CN", "AJXBRY_CN", "AJSTATE", "AB", "XZCS", "ZAGJ", "AJLX", "FADY", "FADD", "ZYAQ", "AJMC", "SSSQ", "RESERVATION29", "WFQK", "JIGUAN", "RYSTATE", "XZDX", "XXDZMS", "HJDZ", "RYWHCD", "MZ", "XM", "ZZDXZQH", "ZBDW_CN", "AJXBRY_CN", "AB", "ZAGJ", "AJLX", "SL_JJFS", "AJSTATE", "XZCS", "GJ", "YWGXBM", "TZMS"};
    private static String[] FIELDS_WP = {"WPMC", "WPXZ", "WPLB", "SSLX", "WPYS", "WPCD_CN", "CPYS", "WPSZ", "SZDZ", "ZHDD", "RESERVATION01", "RESERVATION02", "TZMS", "FADD_JD", "AJZBRY_CN", "DEPARTMENTCODE_CN", "AJLARY_CN", "LADW_CN", "SLJJDW_CN", "ZABZ", "SLJSDW_CN", "XZSJ", "SL_JJFS", "ZBDW_CN", "AJXBRY_CN", "AJSTATE", "AB", "XZCS", "ZAGJ", "AJLX", "FADY", "FADD", "ZYAQ", "AJMC", "SSSQ"};

    public static void main(String[] args) {
       /* Map resultMap = new HashMap();
        Map propertiesMap = new HashMap<>();

        Map itemMap = new HashMap();
        itemMap.put("type", "text");
        itemMap.put("analyzer", ANALYZER);
        itemMap.put("search_analyzer", SEARCH_ANALYZER);

        for (String field : FIELDS_DW) {
            propertiesMap.put(field, itemMap);
        }
        resultMap.put("properties", propertiesMap);
        String proStr = JsonUtil.bean2Json(resultMap);
        System.out.println(proStr);*/
        System.out.println(Arrays.toString(FIELDS_JQ));
    }
}
