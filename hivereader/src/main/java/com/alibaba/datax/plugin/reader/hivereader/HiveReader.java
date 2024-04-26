package com.alibaba.datax.plugin.reader.hivereader;

//import com.alibaba.datax.plugin.reader.hivereader.Key;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.authentication.util.KerberosName;

import java.lang.reflect.Field;
import java.util.List;

import static com.alibaba.datax.plugin.reader.hivereader.Constant.DEFAULT_FETCH_SIZE;//2048，可根据条件自己取值
import static com.alibaba.datax.plugin.reader.hivereader.Key.FETCH_SIZE; // 参数名："fetchSize"

public class HiveReader extends Reader {

    private static final Logger LOG = LoggerFactory.getLogger(Job.class);

    //
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Hive;

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.originalConfig = getPluginJobConf();


            Boolean haveKerberos = this.originalConfig.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                LOG.info("检测到kerberos认证，正在进行认证");
                org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
                String kerberosKeytabFilePath = this.originalConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
                String kerberosPrincipal = this.originalConfig.getString(Key.KERBEROS_PRINCIPAL);
                String krb5Path = this.originalConfig.getString(Key.KERBEROS_CONF_FILE_PATH);

                hadoopConf.set("hadoop.security.authentication", "kerberos");
                hadoopConf.set("hive.security.authentication", "kerberos");
                hadoopConf.set("hadoop.security.authorization", "true");
                System.setProperty("java.security.krb5.conf", krb5Path);
                refreshConfig();
                HiveConnByKerberos.kerberosAuthentication(kerberosPrincipal, kerberosKeytabFilePath, hadoopConf, krb5Path);
            }
            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
//            this.originalConfig = commonRdbmsReaderJob.init(originalConfig);
            this.commonRdbmsReaderJob.init(originalConfig);
        }


        @Override
        public void preCheck() {
            this.commonRdbmsReaderJob.preCheck(originalConfig, DATABASE_TYPE);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(originalConfig);
        }
    }

    public static class Task extends Reader.Task
    {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init()
        {
            this.readerSliceConfig = getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE, getTaskGroupId(), getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender)
        {
            int fetchSize = this.readerSliceConfig.getInt(FETCH_SIZE, DEFAULT_FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(readerSliceConfig, recordSender, getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post()
        {
            this.commonRdbmsReaderTask.post(readerSliceConfig);
        }

        @Override
        public void destroy()
        {
            this.commonRdbmsReaderTask.destroy(readerSliceConfig);
        }
    }
    /** 刷新krb内容信息 */
    public static void refreshConfig() {
        try {
            sun.security.krb5.Config.refresh();
            Field defaultRealmField = KerberosName.class.getDeclaredField("defaultRealm");
            defaultRealmField.setAccessible(true);
            defaultRealmField.set(
                    null,
                    org.apache.hadoop.security.authentication.util.KerberosUtil.getDefaultRealm());
            // reload java.security.auth.login.config
            javax.security.auth.login.Configuration.setConfiguration(null);
        } catch (Exception e) {
            LOG.warn(
                    "resetting default realm failed, current default realm will still be used.", e);
        }
    }
}