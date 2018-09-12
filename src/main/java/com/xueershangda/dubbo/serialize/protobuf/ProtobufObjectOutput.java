package com.xueershangda.dubbo.serialize.protobuf;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.serialize.ObjectOutput;
import io.protostuff.*;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 基于 Protobuf的对象序列化。第一个字节是类型，第 2-5是数据长度。基本类型使用ByteBuffer来处理。对象使用 Protobuf（并且保存对象类型）。
 *
 * @author yinlei
 * @since 2018/9/11 12:29
 */
public class ProtobufObjectOutput implements ObjectOutput {

    private static final Logger LOGGER = LogManager.getLogger(ProtobufObjectOutput.class);

    private OutputStream output;
    private ByteBuffer byteBuffer;

    public ProtobufObjectOutput(URL url, OutputStream output) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Create ProtobufObjectOutput, URL=[{}].", url.toFullString());
        }
        this.byteBuffer = ByteBuffer.allocate(1024);
        this.output = output;
    }

    @Override
    public void writeBool(boolean v) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("writeBool, value=[{}].", v);
        }
        check(2);
        byteBuffer.put((byte) 13); // 数据类型
        byteBuffer.put(v ? (byte)1 : 0);
    }

    @Override
    public void writeByte(byte v) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("writeByte, value=[{}].", v);
        }
        check(2);
        byteBuffer.put((byte) 9);
        byteBuffer.put(v);
    }

    @Override
    public void writeShort(short v) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("writeShort, value=[{}].", v);
        }
        check(3);
        byteBuffer.put((byte) 11);
        byteBuffer.putShort(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("writeInt, value=[{}].", v);
        }
        check(5);
        byteBuffer.put((byte) 4);
        byteBuffer.putInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("writeLong, value=[{}].", v);
        }
        check(9);
        byteBuffer.put((byte) 5);
        byteBuffer.putLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("writeFloat, value=[{}].", v);
        }
        check(5);
        byteBuffer.put((byte) 10);
        byteBuffer.putFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("writeDouble, value=[{}].", v);
        }
        check(9);
        byteBuffer.put((byte) 6);
        byteBuffer.putDouble(v);
    }

    @Override
    public void writeUTF(String v) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("writeUTF(String), value=[{}].", v);
        }
        check(1);
        byteBuffer.put((byte) 12); // 数据类型
        if (v == null) {
            check(4);
            byteBuffer.putInt(0); // 长度为0
        } else {
            byte[] bytes = v.getBytes("UTF-8");
            int len = bytes.length;
            check(4 + len);
            byteBuffer.putInt(len); // int 占 4 位
            byteBuffer.put(bytes); // 存入数据
        }
    }

    @Override
    public void writeBytes(byte[] v) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("writeBytes, byteArrayLength=[{}].", v != null ? v.length : 0);
        }
        if (v == null || v.length == 0) {
            check(5);
            byteBuffer.put((byte) 14);
            byteBuffer.putInt(0);
        } else {
            writeBytes(v, 0 , v.length);
        }
    }

    @Override
    public void writeBytes(byte[] v, int off, int len) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("writeBytes(offset), byteArrayLength=[{}], offset=[{}], len=[{}].",
                    v != null ? v.length : 0, off, len);
        }
        check(1);
        byteBuffer.put((byte) 14);
        if (v == null) {
            check(4);
            byteBuffer.putInt(0);
        } else {
            check(4 + len);
            byteBuffer.putInt(len);
            byteBuffer.put(v, off, len);
        }
    }

    @Override
    public void flushBuffer() throws IOException {
        byteBuffer.flip();
        int limit = byteBuffer.limit();
        byte[] bytes = new byte[limit];
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("flushBuffer, byteArrayLength=[{}].", limit);
        }
        byteBuffer.get(bytes);
        output.write(bytes);
        output.flush();
    }

    private void writeBigNumber(String v, byte type) throws IOException {
        byte[] bytes = v.getBytes("UTF-8");
        int len = bytes.length;
        check(5 + len);
        byteBuffer.put(type);
        byteBuffer.putInt(len);
        byteBuffer.put(bytes);
    }

    @Override
    public void writeObject(Object obj) throws IOException {
        if (obj == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeObject, object is null, maybe Heart beat.");
            }
            writeBool(true);
            return;
        }

        Class cls;
        if (obj instanceof List) {
            List list = (List) obj;
            if (list.isEmpty()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("writeObject(List), List is empty.");
                }
                check(5);
                byteBuffer.put((byte) 1); // 类型
                byteBuffer.putInt(0); // 长度
                return;
            }
            cls = list.get(0).getClass();
//            String className = cls.getName();
//            byte[] classNameBytes = className.getBytes("UTF-8");
//            int classNameLength = classNameBytes.length;
            byte[] dataBytes = collectToBytes(cls, list);
            int dataLength = dataBytes.length;
            // 9 = 1 + 4 + 4
//            int totalLength = 9 + classNameLength + dataLength;
            int totalLength = 5 + dataLength;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeObject(List<{}>), dataLength=[{}].", cls.getName(), totalLength);
            }
            check(totalLength);
            byteBuffer.put((byte) 1); // 类型
            byteBuffer.putInt(totalLength);
//            byteBuffer.putInt(classNameLength);
//            byteBuffer.put(classNameBytes);
            byteBuffer.put(dataBytes);
        } else if (obj instanceof Set) {
            Set set = (Set) obj;
            if (set.isEmpty()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("writeObject(Set), Set is empty.");
                }
                check(5);
                byteBuffer.put((byte) 2); // 类型
                byteBuffer.putInt(0); // 长度
                return;
            }
            cls = set.iterator().next().getClass();
//            String className = cls.getName();
//            byte[] classNameBytes = className.getBytes("UTF-8");
//            int classNameLength = classNameBytes.length;
            byte[] dataBytes = collectToBytes(cls, set);
            int dataLength = dataBytes.length;
            // 9 = 1 + 4 + 4
//            int totalLength = 9 + classNameLength + dataLength;
            int totalLength = 5 + dataLength;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeObject(Set<{}>), dataLength=[{}].", cls.getName(), totalLength);
            }
            check(totalLength);
            byteBuffer.put((byte) 2); // 类型
            byteBuffer.putInt(totalLength);
//            byteBuffer.putInt(classNameLength);
//            byteBuffer.put(classNameBytes);
            byteBuffer.put(dataBytes);
        } else if (obj instanceof Map) {
            Map map = (Map) obj;
            if (map.isEmpty()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("writeObject(Map), Map is empty.");
                }
                check(5);
                byteBuffer.put((byte) 3); // 类型
                byteBuffer.putInt(0); // 长度
                return;
            }
            // value对象的类型
            cls = map.values().iterator().next().getClass();
//            String className = cls.getName();
//            byte[] nameBytes = className.getBytes("UTF-8");
//            int nameLength = nameBytes.length;
            byte[] mapBytes = mapToBytes(cls, map);
            int dataLength = mapBytes.length;
            // 9 = 1 + 4 + 4
//            int totalLength = 9 + nameLength + dataLength;
            int totalLength = 5 + dataLength;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeObject(Map<String, {}>), dataLength=[{}].", cls.getName(), totalLength);
            }
            check(totalLength);
            byteBuffer.put((byte) 3); // 类型
            byteBuffer.putInt(totalLength);
//            byteBuffer.putInt(nameLength);
//            byteBuffer.put(nameBytes);
            byteBuffer.put(mapBytes);
        } else if (obj instanceof Number) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeObject(Number), value=[{}].", obj);
            }
            if (obj instanceof Integer) {
                int v = (int) obj;
                writeInt(v);
            } else if (obj instanceof Long) {
                long l = (long) obj;
                writeLong(l);
            } else if (obj instanceof Double) {
                double d = (double) obj;
                writeDouble(d);
            } else if (obj instanceof BigInteger) {
                BigInteger s = (BigInteger) obj;
                String v = s.toString();
                writeBigNumber(v, (byte) 7);
            } else if (obj instanceof BigDecimal) {
                BigDecimal s = (BigDecimal) obj;
                String v = s.toString();
                writeBigNumber(v, (byte) 8);
            } else if (obj instanceof Byte) {
                byte v = (byte) obj;
                writeByte(v);
            } else if (obj instanceof Float) {
                float v = (float) obj;
                writeFloat(v);
            } else if (obj instanceof Short) {
                short v = (short) obj;
                writeShort(v);
            } else {
                throw new RuntimeException("不支持的数字类型:" + obj.getClass().getName());
            }
        } else if (obj instanceof String) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeObject(String), value=[{}].", obj);
            }
            String v = (String) obj;
            writeUTF(v);
        } else if (obj instanceof Boolean) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeObject(Boolean), value=[{}].", obj);
            }
            boolean b = (boolean) obj;
            writeBool(b);
        } else if (obj instanceof byte[]) {
            byte[] bytes = (byte[]) obj;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeObject(bytes), dataLength=[{}].", bytes.length);
            }
            writeBytes(bytes);
        } else if (obj.getClass().isArray()) {
            // 数组的序列化是支持的，但是性能没有List好，建议使用List代替
            throw new UnsupportedEncodingException("Please use List instead of.");
        } else if (obj instanceof Throwable) {
            // 因为异常的超类Throwable中的cause是引用的自己，有循环引用。目前protostuff还不能处理，fst倒是可以。
            // 所以，自己简单将其序列化，只返回类名和message。
            String className = obj.getClass().getName();
            byte[] nameBytes = className.getBytes("UTF-8");
            Throwable throwable = (Throwable) obj;
            String message = throwable.getMessage();
            byte[] messageBytes = null;
            if (message != null) {
                messageBytes = message.getBytes("UTF-8");
            }
            int nameLength = nameBytes.length;
            int totalLength = nameLength;
            if (messageBytes != null) {
                totalLength += messageBytes.length;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeObject(Throwable<{}>), message=[{}].", className, message);
            }
            check(totalLength + 9);
            byteBuffer.put((byte) 16);
            byteBuffer.putInt(totalLength);
            byteBuffer.putInt(nameLength);
            byteBuffer.put(nameBytes);
            if (messageBytes != null) {
                byteBuffer.put(messageBytes);
            }
        } else {
            cls = obj.getClass();
            Schema schema = RuntimeSchema.getSchema(cls);
            LinkedBuffer buffer = LinkedBuffer.allocate();
            @SuppressWarnings("unchecked")
            byte[] bytes = ProtostuffIOUtil.toByteArray(obj, schema, buffer);
            int length = bytes.length;
//            String className = cls.getName();
//            byte[] nameBytes = className.getBytes("UTF-8");
//            int nameLength = nameBytes.length;
//            int totalLength = 9 + nameLength + length;
            int totalLength = 5 + length;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeObject({}), dataLength=[{}].", cls.getName(), totalLength);
            }
            check(totalLength);
            byteBuffer.put((byte) 0);
            byteBuffer.putInt(totalLength);
//            byteBuffer.putInt(nameLength);
//            byteBuffer.put(nameBytes);
            byteBuffer.put(bytes);
        }
    }

    @SuppressWarnings("unchecked")
    private byte[] mapToBytes(Class clazz, Map map) {
        Schema schema = RuntimeSchema.getSchema(clazz);
        StringMapSchema collectionSchema = new StringMapSchema(schema);
        LinkedBuffer buffer = LinkedBuffer.allocate(1024);
        return ProtostuffIOUtil.toByteArray(map, collectionSchema, buffer);
    }

    @SuppressWarnings("unchecked")
    private byte[] collectToBytes(Class clazz, Collection list) {
        Schema schema = RuntimeSchema.getSchema(clazz);
        MessageCollectionSchema collectionSchema = new MessageCollectionSchema(schema);
        LinkedBuffer buffer = LinkedBuffer.allocate(1024);
        return ProtostuffIOUtil.toByteArray(list, collectionSchema, buffer);
    }

    /**
     * 检查buffer中的剩余空间是否能放下新加入的数据，不行就扩容。
     * 扩容的大小为（size + 256）。
     *
     * @param size buffer要新加入的数据大小
     */
    private void check(int size) {
        if (byteBuffer.remaining() < size) {
            int cap = byteBuffer.capacity() + size + 256;
            ByteBuffer buffer = ByteBuffer.allocate(cap);
            byteBuffer.flip();
            buffer.put(byteBuffer);
            byteBuffer = buffer;
        }
    }
}
