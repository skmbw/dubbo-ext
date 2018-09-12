package com.xueershangda.dubbo.serialize.protobuf;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.serialize.ObjectInput;
import com.alibaba.dubbo.common.serialize.ObjectOutput;
import com.alibaba.dubbo.common.serialize.Serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author yinlei
 * @since 2018/9/11 16:27
 */
public class ProtobufSerialization implements Serialization {
    public byte getContentTypeId() {
        return 13;
    }

    public String getContentType() {
        return "x-application/protobuf";
    }

    public ObjectOutput serialize(URL url, OutputStream output) throws IOException {
        return new ProtobufObjectOutput(url, output);
    }

    public ObjectInput deserialize(URL url, InputStream input) throws IOException {
        return new ProtobufObjectInput(url, input);
    }
}
