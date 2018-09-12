package com.xueershangda.dubbo.serialize.protobuf;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.compiler.support.ClassUtils;
import com.alibaba.dubbo.common.serialize.ObjectInput;
import io.protostuff.*;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author yinlei
 * @since 2018/9/11 12:28
 */
public class ProtobufObjectInput implements ObjectInput {

    private static final Logger LOGGER = LogManager.getLogger(ProtobufObjectInput.class);

    private byte[] bytes;
    private ByteBuffer byteBuffer;
    private ByteArrayInput input;

    public ProtobufObjectInput(URL url, InputStream inputStream) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("构造 ProtobufObjectInput, URL=[{}].", url.toFullString());
        }
        bytes = IOUtils.toByteArray(inputStream);
        input = new ByteArrayInput(bytes, 0, bytes.length, false);
        byteBuffer = ByteBuffer.wrap(bytes);
    }

    public boolean readBool() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readBool.");
        }
        return input.readBool();
    }

    public byte readByte() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readByte.");
        }
        return (byte) input.readInt32();
    }

    public short readShort() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readShort.");
        }
        return (short) input.readInt32();
    }

    public int readInt() throws IOException {
        int i = input.readInt32();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readInt, value=[{}].", i);
        }
        return i;
    }

    public long readLong() throws IOException {
        long l = input.readInt64();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readLong, value=[{}].", l);
        }
        return l;
    }

    public float readFloat() throws IOException {
        float f = input.readFloat();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readFloat, value=[{}].", f);
        }
        return f;
    }

    public double readDouble() throws IOException {
        double d = input.readDouble();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readDouble, value=[{}].", d);
        }
        return d;
    }

    public String readUTF() throws IOException {
        String s = input.readString();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readUTF, value=[{}].", s);
        }
        return s;
    }

    public byte[] readBytes() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readBytes.");
        }
        return input.readByteArray();
    }

    private String readString() throws IOException {
        int length = byteBuffer.getInt();
        if (length != 0) {
            byte[] data = new byte[length];
            byteBuffer.get(data);
            return new String(data, "UTF-8");
        }
        return null;
    }

    private byte[] getBytes() throws IOException {
        int length = byteBuffer.getInt();
        if (length != 0) {
            byte[] data = new byte[length];
            byteBuffer.get(data);
            return data;
        }
        return new byte[0];
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object readObject() throws IOException, ClassNotFoundException {
        if (bytes == null || bytes.length == 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("readObject, byteArray is null, return null.");
            }
            return null;
        }

        byte type = byteBuffer.get();
        // 基本类型和复合类型在一起，导致获取数据长度有问题
        switch (type) {
            case 4:
                return byteBuffer.getInt();
            case 5:
                return byteBuffer.getLong();
            case 6:
                return byteBuffer.getDouble();
            case 7:
                String s = readString();
                return new BigInteger(s == null ? "0" : s);
            case 8:
                s = readString();
                return new BigDecimal(s == null ? "0" : s);
            case 9:
                // 标志位 不想 再重置了
                return byteBuffer.get();
            case 10:
                return byteBuffer.getFloat();
            case 11:
                return byteBuffer.getShort();
            case 12:
                return readString();
            case 13:
                return byteBuffer.get() != 0;
            case 14:
                return getBytes();
//            case 15: // 数组的序列化是支持的，但是性能没有List好，建议使用List代替
//                throw new UnsupportedEncodingException("Please use List instead of.");
            case 16: // 异常
                int totalLength = byteBuffer.getInt();
                int nameLength = byteBuffer.getInt();
                byte[] classNameBytes = new byte[nameLength];
                byteBuffer.get(classNameBytes);
                String message = "";
                if (totalLength > nameLength) {
                    int messageLength = totalLength - nameLength;
                    byte[] messageBytes = new byte[messageLength];
                    byteBuffer.get(messageBytes);
                    message = new String(messageBytes, "UTF-8");
                }
                String className = new String(classNameBytes, "UTF-8");
                return new RuntimeException(className + ";message=" + message);
        }

        // 集合和对象类型和基本类型分开，代码更整洁
        int totalLength = byteBuffer.getInt();
        if (totalLength == 0) {
            switch (type) {
                case 0: // 对象
                    return null;
                case 1:
                    return Collections.emptyList();
                case 2:
                    return Collections.emptySet();
                case 3:
                    return Collections.emptyMap();
            }
        }
        int nameLength = byteBuffer.getInt();
        byte[] nameBytes = new byte[nameLength];
        byteBuffer.get(nameBytes);
        String className = new String(nameBytes, "UTF-8");
        Class clazz = ClassUtils.forName(className);
        Schema schema = RuntimeSchema.getSchema(clazz);

        byte[] dataBytes = new byte[totalLength - nameLength - 9];
        byteBuffer.get(dataBytes);
        switch (type) {
            case 0:
                Object object = newInstance(clazz);
                ProtostuffIOUtil.mergeFrom(dataBytes, object, schema);
                return object;
            case 1:
                MessageCollectionSchema collectionSchema = new MessageCollectionSchema(schema);
                List list = new ArrayList();
                ProtostuffIOUtil.mergeFrom(dataBytes, list, collectionSchema);
                return list;
            case 2:
                collectionSchema = new MessageCollectionSchema(schema);
                Set set = new HashSet();
                ProtostuffIOUtil.mergeFrom(dataBytes, set, collectionSchema);
                return set;
            case 3:
                StringMapSchema stringSchema = new StringMapSchema(schema);
                Map map = new HashMap();
                ProtostuffIOUtil.mergeFrom(dataBytes, map, stringSchema);
                return map;
            default:

        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readObject(Class<T> cls) throws IOException, ClassNotFoundException {
        return (T) readObject();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readObject(Class<T> cls, Type type) throws IOException, ClassNotFoundException {
        return (T) readObject();
    }

    private Object newInstance(Class clazz) {
        Object entity;
        try {
            entity = clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("实例化对象的异常", e);
        }
        return entity;
    }
}
