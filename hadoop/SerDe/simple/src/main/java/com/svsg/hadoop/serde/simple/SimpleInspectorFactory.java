package com.svsg.hadoop.serde.simple;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleInspectorFactory {

    static ConcurrentHashMap<TypeInfo, ObjectInspector> cachedPrimitiveObjectInspector = new ConcurrentHashMap<TypeInfo, ObjectInspector>();

    /**
     * In this factory,we extract every Inspector we need and add it to our SerDe inspector. for this purpose we need type of each column to find a propert inspector
     * and columns name for handling the inquiry for the fields.
     * @param columnNames name of columns
     * @param typeInfos type of the fields
     * @return inspector of SerDe
     */
    public static StructObjectInspector createStructInspector(List<String> columnNames, List<TypeInfo> typeInfos) {

        ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(typeInfos.size());
        for (TypeInfo typeInfo : typeInfos) {
            columnObjectInspectors.add(SimpleInspectorFactory.createObjectInspector(typeInfo));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnObjectInspectors);

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
            case LIST:
                SimpleInspectorFactory.createObjectInspector(((ListTypeInfo) typeInfo).getListElementTypeInfo());
                break;
            case MAP:
                break;
            case STRUCT:
                break;
            case UNION:
                break;
        }
        return cachedPrimitiveObjectInspector.put(typeInfo, inspector);
    }

    private static ObjectInspector getPrimitiveJavaObjectInspector(PrimitiveCategory primitiveCategory) {
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveCategory);
    }
}
