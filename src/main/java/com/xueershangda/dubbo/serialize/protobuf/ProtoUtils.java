package com.xueershangda.dubbo.serialize.protobuf;

import com.vteba.utils.reflection.ReflectUtils;
import io.protostuff.*;
import io.protostuff.runtime.RuntimeSchema;
import org.springframework.objenesis.ObjenesisHelper;

import javax.validation.constraints.NotNull;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Protobuf 序列化工具
 *
 * @author yinlei
 * @since 2018/9/11 13:01
 */
public class ProtoUtils {

    private static final byte[] ZERO = new byte[0];

    @NotNull
    public static byte[] toByteArray(Object object) {
        if (object == null) {
            return ZERO; // 返回null或者byte[0]，都是可以的，调用的地方要做处理
        }
        LinkedBuffer buffer = LinkedBuffer.allocate(1024);
        byte[] bytes;
        Class clazz;
        if (object instanceof Collection) {
            Collection collection = getType(object);
            if (collection.isEmpty()) {
                return ZERO;
            }
            // 集合为空时，这里会有异常
            clazz = collection.iterator().next().getClass();
            bytes = collectToBytes(clazz, collection);
        } else if (object instanceof Map) {
            Map<String, Object> map = getType(object);
            if (map.isEmpty()) {
                return ZERO;
            }
            // Map为空时，这里会有异常
            clazz = map.values().iterator().next().getClass();
            bytes = mapToBytes(clazz, map);
        } else {
            clazz = object.getClass();
            Schema schema = RuntimeSchema.getSchema(clazz);
            bytes = ProtobufIOUtil.toByteArray(object, schema, buffer);
        }
        return bytes;
    }

    /**
     * 将字节数组反序列化成对象。和toByteArray配对使用，中间不再传递类型信息。
     *
     * @param bytes 待反序列化的字节数组
     * @param type  反序列化后的对象类型
     * @param <T>   返回类型泛型
     * @param <E>   如果是集合或Map，E是其中的元素类型
     * @return T对象
     */
    @SuppressWarnings("unchecked")
    public static <T, E> T fromByteArray(byte[] bytes, Type type) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        // 这里使用Class<T>更符合逻辑，因为泛型擦除，其实是无所谓的，而且对后面泛型转换有好处，不用再转了
        Class<T> rawClass = ReflectUtils.getRawType(type);

        if (List.class.isAssignableFrom(rawClass)) {
            Class<E> genericClass = ReflectUtils.getGenericClass(type);
            Schema<E> schema = RuntimeSchema.getSchema(genericClass);
            MessageCollectionSchema<E> listSchema = new MessageCollectionSchema<>(schema);
            List<E> list = new ArrayList<>();
            ProtobufIOUtil.mergeFrom(bytes, list, listSchema);
            return (T) list;
        } else if (Map.class.isAssignableFrom(rawClass)) {
            Class<E> genericClass = ReflectUtils.getGenericClass(type, 1);
            Schema<E> schema = RuntimeSchema.getSchema(genericClass);
            StringMapSchema<E> stringSchema = new StringMapSchema<>(schema);
            Map<String, E> map = new HashMap<>();
            ProtobufIOUtil.mergeFrom(bytes, map, stringSchema);
            return (T) map;
        } else if (Set.class.isAssignableFrom(rawClass)) {
            Class<E> genericClass = ReflectUtils.getGenericClass(type);
            Schema<E> schema = RuntimeSchema.getSchema(genericClass);
            MessageCollectionSchema<E> setSchema = new MessageCollectionSchema<>(schema);
            Set<E> set = new HashSet<>();
            ProtobufIOUtil.mergeFrom(bytes, set, setSchema);
            return (T) set;
        } else {
            Class<E> genericClass = ReflectUtils.getGenericClass(type);
            Schema<E> schema = RuntimeSchema.getSchema(genericClass);
            E entity = ObjenesisHelper.newInstance(genericClass);
            ProtobufIOUtil.mergeFrom(bytes, entity, schema);
            return (T) entity;
        }

    }

    @SuppressWarnings("unchecked")
    private static <T> T getType(Object object) {
        return (T) object;
    }

    private static byte[] mapToBytes(Class clazz, Map<String, Object> map) {
        Schema schema = RuntimeSchema.getSchema(clazz);
        StringMapSchema<Object> collectionSchema = new StringMapSchema(schema);
        LinkedBuffer buffer = LinkedBuffer.allocate(1024);
        return ProtobufIOUtil.toByteArray(map, collectionSchema, buffer);
    }

    private static byte[] collectToBytes(Class clazz, Collection list) {
        Schema schema = RuntimeSchema.getSchema(clazz);
        MessageCollectionSchema collectionSchema = new MessageCollectionSchema(schema);
        LinkedBuffer buffer = LinkedBuffer.allocate(1024);
        return ProtobufIOUtil.toByteArray(list, collectionSchema, buffer);
    }
}
