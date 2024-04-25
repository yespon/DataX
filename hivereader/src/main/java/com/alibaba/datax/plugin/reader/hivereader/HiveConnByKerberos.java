package com.alibaba.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveConnByKerberos {
    private static final Logger LOG = LoggerFactory.getLogger(HiveReader.Job.class);
    public static void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath, org.apache.hadoop.conf.Configuration hadoopConf,String krb5conf) {
        System.setProperty("java.security.krb5.conf",krb5conf);
        if (StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            }
            catch (Exception e) {

                LOG.error("kerberos认证失败");
                String message = String.format("kerberos认证失败，请检查 " +
                                "kerberosKeytabFilePath[%s] 和 kerberosPrincipal[%s]",
                        kerberosKeytabFilePath, kerberosPrincipal);
                e.printStackTrace();
                throw DataXException.asDataXException(HiveReaderErrorCode.KERBEROS_LOGIN_ERROR, message, e);
            }
        }
    }
}
