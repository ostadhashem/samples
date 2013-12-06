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

/**
 * This is a main class that we should introduce it to Hive for using as a SerDe.
 * It should extend {@link AbstractSerDe}
 */
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

    /**
     * This is where we should initialize our SerDe, initialize our Inspector and everything we need should be get from parameters,
     * @param entries configuration sent for SerDe
     * @param properties table properties, remember that delimiter and others are here
     * @throws SerDeException
     */
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

    /**
     * We show that how we will save our object in a {@link Writable} object
     * @param object the input object, its type usually is same as deserialize method return type.
     * @param objectInspector inspector we defined for our SerDe.
     * @return a writable object
     * @throws SerDeException in case of error
     */
    @Override
    public Writable serialize(Object object, ObjectInspector objectInspector) throws SerDeException {
        LOG.info("In Serialization&&&&&");
        Text write = new Text();
        StringBuilder row = new StringBuilder();
        @SuppressWarnings("unchecked")
        ArrayList<Object> inputObject = (ArrayList<java.lang.Object>) object;
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

    /**
     * Deserialize the writable data to target object.
     * @param writable data
     * @return the object we expect to work with that
     * @throws SerDeException in case of error
     */
    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        LOG.info("In deserialization &&&&&");
        String input = writable.toString();
        deSerializedDataSize = input.length();
        ArrayList<Object> rowData = new ArrayList<Object>();
        String[] fields = input.split("\u0002");
        for (int i = 0; i < serdeParams.getColumnNames().size(); i++) {
            rowData.add(deserialize((PrimitiveTypeInfo) serdeParams.getColumnTypes().get(i), fields[i]));
        }
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

    /**
     * The ObjectInspector used in this SerDe. usually we should use a factory to create that , and cache the instance for further usage,
     * Usually an objectInspector contains some other ObjectInspector. For example if you want have some String field in your table
     * and want to convert it as an object that contains these field, you should have some JavaStringObjectInspector, in your inspector.
     * @return objectInspector of this SerDe.
     * @throws SerDeException
     */
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }
}
