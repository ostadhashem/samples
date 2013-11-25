package com.svsg.hadoop.serde.simple;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleInspectorFactory {

    static ConcurrentHashMap<TypeInfo, ObjectInspector> cachedPrimitiveObjectInspector = new ConcurrentHashMap<TypeInfo, ObjectInspector>();
    static ConcurrentHashMap<ArrayList<Object>, SimpleInspector> cachedSimpleObjectInspector = new ConcurrentHashMap<ArrayList<Object>, SimpleInspector>();

    public static SimpleInspector createStructInspector(List<String> columnNames, List<TypeInfo> typeInfos) {

        ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(typeInfos.size());
        for (TypeInfo typeInfo : typeInfos) {
            columnObjectInspectors.add(SimpleInspectorFactory.createObjectInspector(typeInfo));
        }
        return (createObjectInspector(columnNames, columnObjectInspectors, typeInfos));

    }

    private static SimpleInspector createObjectInspector(List<String> columnNames,
                                                         ArrayList<ObjectInspector> columnObjectInspectors,
                                                         List<TypeInfo> typeInfos) {
        ArrayList<Object> key = new ArrayList<Object>();
        key.add(columnNames);
        key.add(typeInfos);
        if (cachedSimpleObjectInspector.contains(key)) {
            return cachedSimpleObjectInspector.get(key);
        }
        return cachedSimpleObjectInspector.put(key, new SimpleInspector(columnNames, columnObjectInspectors));
    }

    private static ObjectInspector createObjectInspector(TypeInfo typeInfo) {
        ObjectInspector inspector = cachedPrimitiveObjectInspector.get(typeInfo);
        if (inspector != null) {
            return inspector;
        }
        switch (typeInfo.getCategory()) {

            case PRIMITIVE:
                PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;

                inspector = SimpleInspectorFactory.getPrimitiveJavaObjectInspector(
                        pti.getPrimitiveCategory().equals(PrimitiveCategory.FLOAT) ?
                                PrimitiveCategory.DOUBLE : pti.getPrimitiveCategory());
                break;
            default:
                throw new IllegalArgumentException("Simple SerDe only support primitive types");
        }
        return cachedPrimitiveObjectInspector.put(typeInfo, inspector);
    }

    private static ObjectInspector getPrimitiveJavaObjectInspector(PrimitiveCategory primitiveCategory) {
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveCategory);
    }
}
