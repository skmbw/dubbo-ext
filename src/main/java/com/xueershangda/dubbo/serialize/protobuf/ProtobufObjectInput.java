package com.xueershangda.dubbo.serialize.protobuf;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.serialize.ObjectInput;
import com.vteba.utils.reflection.ReflectUtils;
import io.protostuff.MessageCollectionSchema;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.StringMapSchema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.objenesis.ObjenesisHelper;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Protobuf 数据反序列化。第一个字节是类型，第 2-5是数据长度。基本类型使用ByteBuffer来处理。对象使用 Protobuf。
 *
 * @author yinlei
 * @since 2018/9/11 12:28
 */
public class ProtobufObjectInput implements ObjectInput {

    private static final Logger LOGGER = LogManager.getLogger(ProtobufObjectInput.class);

    private byte[] bytes;
    private ByteBuffer byteBuffer;

    public ProtobufObjectInput(URL url, InputStream inputStream) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Create ProtobufObjectInput, URL=[{}].", url.toFullString());
        }
        bytes = IOUtils.toByteArray(inputStream);
        byteBuffer = ByteBuffer.wrap(bytes);
    }

    public boolean readBool() throws IOException {
        byteBuffer.get();
        boolean b = byteBuffer.get() != 0;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readBool, value=[{}].", b);
        }
        return b;
    }

    public byte readByte() throws IOException {
        byteBuffer.get();
        byte b = byteBuffer.get();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readByte value=[{}].", b);
        }
        return b;
    }

    public short readShort() throws IOException {
        byteBuffer.get();
        short s = byteBuffer.getShort();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readShort value=[{}].", s);
        }
        return s;
    }

    public int readInt() throws IOException {
        byteBuffer.get();
        int i = byteBuffer.getInt();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readInt, value=[{}].", i);
        }
        return i;
    }

    public long readLong() throws IOException {
        byteBuffer.get();
        long l = byteBuffer.getLong();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readLong, value=[{}].", l);
        }
        return l;
    }

    public float readFloat() throws IOException {
        byteBuffer.get();
        float f = byteBuffer.getFloat();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readFloat, value=[{}].", f);
        }
        return f;
    }

    public double readDouble() throws IOException {
        byteBuffer.get();
        double d = byteBuffer.getDouble();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readDouble, value=[{}].", d);
        }
        return d;
    }

    public String readUTF() throws IOException {
        byte type = byteBuffer.get();
        if (type != 12) { // 不是string类型
            return null;
        }
        String s = readString();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readUTF, value=[{}].", s);
        }
        return s;
    }

    public byte[] readBytes() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readBytes.");
        }
        byte type = byteBuffer.get();
        if (type != 14) { // 不是byte[]
            return new byte[0];
        }
        return readByteArray();
    }

    private String readString() throws IOException {
        int length = byteBuffer.getInt();
        if (length != 0) {
            byte[] data = new byte[length];
            byteBuffer.get(data);
            String s = new String(data, "UTF-8");
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("readString, s=[{}].", s);
            }
            return s;
        }
        return null;
    }

    private byte[] readByteArray() throws IOException {
        int length = byteBuffer.getInt();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readByteArray, dataLength=[{}].", length);
        }
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
        if (bytes.length == 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("readObject, byteArray is empty, return null.");
            }
            return null;
        }

        byte type = byteBuffer.get();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("readObject, dataType=[{}].", type);
        }
        // 基本类型和复合类型在一起，导致获取数据长度有问题
        switch (type) {
            case 4:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, int.");
                }
                return byteBuffer.getInt();
            case 5:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, long.");
                }
                return byteBuffer.getLong();
            case 6:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, double.");
                }
                return byteBuffer.getDouble();
            case 7:
                // 已经读取过标志位了，不再去mark reset，接着读
                String s = readString();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, BigInteger.");
                }
                return new BigInteger(s == null ? "0" : s);
            case 8:
                s = readString();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, BigDecimal.");
                }
                return new BigDecimal(s == null ? "0" : s);
            case 9:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, byte.");
                }
                return byteBuffer.get();
            case 10:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, float.");
                }
                return byteBuffer.getFloat();
            case 11:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, short.");
                }
                return byteBuffer.getShort();
            case 12:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, String.");
                }
                return readString();
            case 13:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, boolean.");
                }
                return byteBuffer.get() != 0;
            case 14:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, byte array.");
                }
                return readByteArray();
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
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject, Throwable[{}], message=[{}].", className, message);
                }
                return new RuntimeException(className + ";message=" + message);
        }
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn("readObject, unknown data type=[{}], skip and return null.", type);
        }
        return null;
    }

    // 主要用于接收方法参数
    @Override
    @SuppressWarnings("unchecked")
    public <T> T readObject(Class<T> cls) throws IOException, ClassNotFoundException {
        if (LOGGER.isDebugEnabled()) {
            Type[] types = ReflectUtils.getGenericTypeArray(cls);
            LOGGER.debug("readObject(Class<{}>), type=[{}].", cls.getName(), types);
        }
        return readObject(cls, cls);
    }

    // 主要用于接收返回结果
    @Override
    @SuppressWarnings("unchecked")
    public <T> T readObject(Class<T> cls, Type type) throws IOException, ClassNotFoundException {
        if (LOGGER.isDebugEnabled()) {
            Class<?> clazz = ReflectUtils.getGenericClass(type);
            LOGGER.debug("readObject(Class<{}>), GenericClass=[{}], type=[{}].",
                    cls.getName(), clazz, type.getTypeName());
        }
        byteBuffer.mark(); // 如果没有处理到，reset回来
        byte dataType = byteBuffer.get();
        // 基本类型和异常
        if (dataType > 3) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("readObject(cls), primitive Type, use readObject instead of.");
            }
            byteBuffer.reset();
            return (T) readObject();
        }

        // 集合和对象类型和基本类型分开，代码更整洁
        int totalLength = byteBuffer.getInt();
        if (totalLength == 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("readObject, Non primitive Type, empty Object, dataType=[{}].", dataType);
            }
            switch (dataType) {
                case 0: // 对象
                    return null;
                case 1:
                    return (T) Collections.emptyList();
                case 2:
                    return (T) Collections.emptySet();
                case 3:
                    return (T) Collections.emptyMap();
            }
        }

        int nameLength = byteBuffer.getInt();
        byte[] nameBytes = new byte[nameLength];
        // 可以直接指定position跳过，后面再更改
        byteBuffer.get(nameBytes);

        byte[] dataBytes = new byte[totalLength - nameLength - 9];
        byteBuffer.get(dataBytes);
        switch (dataType) {
            case 0:
                // 是POJO，不用再获取类型信息了，就是她
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject(cls), className=[{}].", cls);
                }
                Schema<T> schema = RuntimeSchema.getSchema(cls);
                T entity = ObjenesisHelper.newInstance(cls);
                ProtostuffIOUtil.mergeFrom(dataBytes, entity, schema);
                return entity;
            case 1:
                Class<T> genericClass = ReflectUtils.getGenericClass(type);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject(cls), className=[List<{}>].", genericClass);
                }
                schema = RuntimeSchema.getSchema(genericClass);
                MessageCollectionSchema collectionSchema = new MessageCollectionSchema(schema);
                List list = new ArrayList();
                ProtostuffIOUtil.mergeFrom(dataBytes, list, collectionSchema);
                return (T) list;
            case 2:
                genericClass = ReflectUtils.getGenericClass(type);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject(cls), className=[Set<{}>].", genericClass);
                }
                schema = RuntimeSchema.getSchema(genericClass);
                collectionSchema = new MessageCollectionSchema(schema);
                Set set = new HashSet();
                ProtostuffIOUtil.mergeFrom(dataBytes, set, collectionSchema);
                return (T) set;
            case 3:
                if (cls == Map.class) { // 是Map接口，没有泛型信息
                    genericClass = (Class<T>) String.class;
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("readObject(cls), the cls is Map interface, will use String instead of.");
                    }
                } else {
                    genericClass = ReflectUtils.getGenericClass(type, 1);
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("readObject(cls), className=[Map<String, {}>].", genericClass);
                }
                schema = RuntimeSchema.getSchema(genericClass);
                StringMapSchema stringSchema = new StringMapSchema(schema);
                Map map = new HashMap();
                ProtostuffIOUtil.mergeFrom(dataBytes, map, stringSchema);
                return (T) map;
            default:
                byteBuffer.reset();
        }
        return (T) readObject();
    }
}
