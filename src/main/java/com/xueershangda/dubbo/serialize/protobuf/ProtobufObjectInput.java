package com.xueershangda.dubbo.serialize.protobuf;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.serialize.ObjectInput;
import io.protostuff.ByteArrayInput;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;

/**
 * @author yinlei
 * @since 2018/9/11 12:28
 */
public class ProtobufObjectInput implements ObjectInput {

    private ByteArrayInput input;
    private byte[] buffer;

    public ProtobufObjectInput(URL url, InputStream stream) throws IOException {
        byte[] buffer = IOUtils.toByteArray(stream);
        this.buffer = buffer;
        input = new ByteArrayInput(buffer, 0, 0, false);
    }

    public Object readObject() throws IOException, ClassNotFoundException {
        return null;
    }

    public <T> T readObject(Class<T> cls) throws IOException, ClassNotFoundException {
        return ProtoUtils.fromByteArray(buffer, cls);
    }

    public <T> T readObject(Class<T> cls, Type type) throws IOException, ClassNotFoundException {
        return ProtoUtils.fromByteArray(buffer, type);
    }

    public boolean readBool() throws IOException {
        return input.readBool();
    }

    public byte readByte() throws IOException {
        return (byte) input.readInt32();
    }

    public short readShort() throws IOException {
        return (short) input.readInt32();
    }

    public int readInt() throws IOException {
        return input.readInt32();
    }

    public long readLong() throws IOException {
        return input.readInt64();
    }

    public float readFloat() throws IOException {
        return input.readFloat();
    }

    public double readDouble() throws IOException {
        return input.readDouble();
    }

    public String readUTF() throws IOException {
        return input.readString();
    }

    public byte[] readBytes() throws IOException {
        return input.readByteArray();
    }
}
