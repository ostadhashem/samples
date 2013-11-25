package com.svsg.hadoop.serde.simple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.math.BigDecimal;
import java.util.*;

@SuppressWarnings("UnusedDeclaration")
public class SimpleSerDe extends AbstractSerDe {
    LazySimpleSerDe.SerDeParameters serdeParams = null;
    StructObjectInspector inspector = null;

    public static final Log LOG = LogFactory.getLog(SimpleSerDe.class
            .getName());
    private int deSerializedDataSize;
    private boolean lastOperationSerialize;
    private boolean lastOperationDeSerialize;
    private SerDeStats stats = new SerDeStats();
    private int serializeDataSize;

    @Override
    public void initialize(Configuration entries, Properties properties) throws SerDeException {
        LOG.info("In Initialization &&&&&&&");
        serdeParams = new LazySimpleSerDe.SerDeParameters();
        LazyUtils.extractColumnInfo(properties, serdeParams, SimpleSerDe.class.getName());
        inspector = SimpleInspectorFactory.createStructInspector(serdeParams.getColumnNames(), serdeParams.getColumnTypes());


        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        LOG.info("In Serialization&&&&&");
        Text write = new Text();
        StringBuilder row = new StringBuilder();
        @SuppressWarnings("unchecked")
        ArrayList<Object> inputObject = (ArrayList<java.lang.Object>) o;
        List<String> columnNames = serdeParams.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            Object columnValue = inputObject.get(i);
            if (columnValue != null) {
                row.append(columnValue.toString());
            }
            if (i < columnNames.size() - 1) {
                row.append("\u0001");
            }
        }
        write.set(row.toString());
        serializeDataSize = row.toString().length();
        lastOperationSerialize = true;
        lastOperationDeSerialize = false;
        return write;
    }

    @Override
    public SerDeStats getSerDeStats() {
        assert (lastOperationSerialize != lastOperationDeSerialize);
        if (lastOperationSerialize) {
            stats.setRawDataSize(serializeDataSize);

        } else {
            stats.setRawDataSize(deSerializedDataSize);
        }

        return stats;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        LOG.info("In deserialization &&&&&");
        String input = writable.toString();
        deSerializedDataSize = input.length();
//        String lines[] = input.split("\n");
//        List<Map<String, Object>> data = new ArrayList<Map<String, Object>>(lines.length);
//        for (String line : lines) {
        ArrayList<Object> rowData = new ArrayList<Object>();
        String[] fields = input.split("\u0002");
        for (int i = 0; i < serdeParams.getColumnNames().size(); i++) {
            rowData.add(deserialize((PrimitiveTypeInfo) serdeParams.getColumnTypes().get(i), fields[i]));
        }
//            data.add(rowData);
//        }
        lastOperationSerialize = false;
        lastOperationDeSerialize = true;
        return rowData;
    }

    private Object deserialize(PrimitiveTypeInfo typeInfo, String field) {
        switch (typeInfo.getPrimitiveCategory()) {
            case VOID:
                return null;
            case BOOLEAN:
                return Boolean.parseBoolean(field);
            case BYTE:
                return Byte.parseByte(field);
            case SHORT:
                return Short.parseShort(field);
            case INT:
                return Integer.parseInt(field);
            case LONG:
                return Long.parseLong(field);
            case FLOAT:
            case DOUBLE:
                return Double.parseDouble(field);
            case STRING:
                return field;
            case DECIMAL:
                return new BigDecimal(field);
            case VARCHAR:
                return field;
            default:
                LOG.error("Can't support data type");
                throw new IllegalArgumentException("Unknown or unsupported type :" + typeInfo.getPrimitiveCategory().toString());
        }
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }
}
