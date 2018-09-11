package com.xueershangda.dubbo.serialize.protobuf;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.serialize.ObjectOutput;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufOutput;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author yinlei
 * @since 2018/9/11 12:29
 */
public class ProtobufObjectOutput implements ObjectOutput {

    private ProtobufOutput output;
    private OutputStream stream;

    public ProtobufObjectOutput(URL url, OutputStream stream) {
        this.stream = stream;
        this.output = new ProtobufOutput(LinkedBuffer.allocate());
    }

    public void writeObject(Object obj) throws IOException {
        byte[] byteArray = ProtoUtils.toByteArray(obj);
        output.writeByteArray(1, byteArray, false);
    }

    public void writeBool(boolean v) throws IOException {
        output.writeBool(1, v, false);
    }

    public void writeByte(byte v) throws IOException {
        output.writeInt32(1, v, false);
    }

    public void writeShort(short v) throws IOException {
        output.writeInt32(1, v, false);
    }

    public void writeInt(int v) throws IOException {
        output.writeInt32(1, v, false);
    }

    public void writeLong(long v) throws IOException {
        output.writeInt64(1, v, false);
    }

    public void writeFloat(float v) throws IOException {
        output.writeFloat(1, v, false);
    }

    public void writeDouble(double v) throws IOException {
        output.writeDouble(1, v, false);
    }

    public void writeUTF(String v) throws IOException {
        output.writeString(1, v, false);
    }

    public void writeBytes(byte[] v) throws IOException {
        output.writeByteArray(1, v, false);
    }

    public void writeBytes(byte[] v, int off, int len) throws IOException {
        output.writeByteRange(true, 1, v, off, len, false);
    }

    public void flushBuffer() throws IOException {
        byte[] byteArray = output.toByteArray();
        stream.write(byteArray);
        stream.flush();
    }
}
