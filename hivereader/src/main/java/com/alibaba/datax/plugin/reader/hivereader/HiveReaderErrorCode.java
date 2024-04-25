package com.alibaba.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HiveReaderErrorCode implements ErrorCode {
    KERBEROS_LOGIN_ERROR("HiveReader-13", "KERBEROS认证失败");

    private final String code;
    private final String description;

    HiveReaderErrorCode(String code, String description)
    {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode()
    {
        return this.code;
    }

    @Override
    public String getDescription()
    {
        return this.description;
    }

    @Override
    public String toString()
    {
        return String.format("Code:[%s], Description:[%s]. ", this.code, this.description);
    }
}
