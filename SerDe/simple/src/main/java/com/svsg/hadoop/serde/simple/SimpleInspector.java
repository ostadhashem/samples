package com.svsg.hadoop.serde.simple;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleInspector extends StandardStructObjectInspector {

    public SimpleInspector(List<String> structFieldNames, List<ObjectInspector> structFieldObjectInspectors) {
        super(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
        return fields;
    }

    @Override
    public StructField getStructFieldRef(String fieldName) {
        return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        return ((Map) data).get(fieldRef.getFieldName());
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
        if (data == null) {
            return null;
        }
        List<Object> values = new ArrayList<Object>();
        Map entry = (Map) data;
        for (MyField field : fields) {
            values.add(entry.get(field));
        }
        return values;
    }

}
