/*
* This file is part of Hive KV Storage Handler
* Copyright 2012 Alexandre Vilcek (alexandre.vilcek@oracle.com)
*
*   Licensed under the Apache License, Version 2.0 (the "License");
*   you may not use this file except in compliance with the License.
*   You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*/
package org.vilcek.hive.kv;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class KVHiveSerDe implements SerDe {

    private static final Logger log = Logger.getLogger(KVHiveSerDe.class);

    private String kvMajorKeysMapping = null;
    private String kvMinorKeysMapping = null;
    private String[] kvMajorKeysMappingArray = null;
    private String[] kvMinorKeysMappingArray = null;
    private List<String> majorMinorKeys = null;
    private List<String> row = null;
    private int fieldCount = 0;
    private StructObjectInspector objectInspector = null;
    private SerDe jsonSerDe = null;
    private String jsonKey = null;
    private String jsonSerDeClass = null;
    private String jsonKeyMappingColumn = null;

    @Override
    public void initialize(Configuration configuration, Properties properties) throws SerDeException {
        jsonKey = properties.getProperty("kv.avro.json.key");
        jsonKeyMappingColumn = properties.getProperty("kv.avro.json.keys.mapping.column");
        jsonSerDeClass = properties.getProperty("kv.json.serde.class", "org.apache.hive.hcatalog.data.JsonSerDe");

        kvMajorKeysMapping = properties.getProperty("kv.major.keys.mapping");
        kvMajorKeysMappingArray = kvMajorKeysMapping.split(",");
        kvMinorKeysMapping = properties.getProperty("kv.minor.keys.mapping");
        if (kvMinorKeysMapping != null) {
            kvMinorKeysMappingArray = kvMinorKeysMapping.split(",");
        } else {
            kvMinorKeysMappingArray = new String[1];
            kvMinorKeysMappingArray[0] = "value";
        }
        fieldCount = kvMajorKeysMappingArray.length + kvMinorKeysMappingArray.length;
        majorMinorKeys = new ArrayList<String>(fieldCount);
        majorMinorKeys.addAll(Arrays.asList(kvMajorKeysMappingArray));
        majorMinorKeys.addAll(Arrays.asList(kvMinorKeysMappingArray));

        if (jsonKey == null) {
            List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            }
            objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(majorMinorKeys, fieldOIs);
            row = new ArrayList<String>(fieldCount);
        } else {
            jsonSerDe = getJsonSerDe();
            jsonSerDe.initialize(configuration, properties);
        }
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        MapWritable input = (MapWritable) writable;
        if (jsonSerDe == null) {
            Text t = new Text();
            row.clear();
            for (int i = 0; i < fieldCount; i++) {
                t.set(majorMinorKeys.get(i));
                Writable value = input.get(t);
                if (value != null && !NullWritable.get().equals(value)) {
                    row.add(value.toString());
                } else {
                    row.add(null);
                }
            }
            return row;
        } else {
            if (jsonKeyMappingColumn != null) {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    ObjectNode keys = mapper.createObjectNode();
                    for (String majorMinorKey : majorMinorKeys) {
                        if (!majorMinorKey.equals(jsonKey)) {
                            Writable key = input.get(new Text(majorMinorKey));
                            if (key != null && !NullWritable.get().equals(key)) {
                                keys.put(majorMinorKey, ((Text) key).toString());
                            }
                        }
                    }
                    Text value = (Text) input.get(new Text(jsonKey));
                    JsonNode root = mapper.readTree(new ByteArrayInputStream((value.getBytes())));
                    ((ObjectNode) root).put(jsonKeyMappingColumn, keys);
                    return jsonSerDe.deserialize(new Text(root.toString()));
                } catch (Exception e) {
                    throw new SerDeException(e);

                }
            } else {
                Text key = new Text();
                key.set(jsonKey);
                return jsonSerDe.deserialize(input.get(key));
            }
        }
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return jsonSerDe == null ? objectInspector : jsonSerDe.getObjectInspector();
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return MapWritable.class;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return new SerDeStats();
    }

    @Override
    public Writable serialize(Object o, ObjectInspector oi) throws SerDeException {
        return null;
    }

    private SerDe getJsonSerDe() {
        try {
            return (SerDe) Class.forName(jsonSerDeClass,
                true,
                Thread.currentThread().getContextClassLoader() == null ? getClass().getClassLoader() : Thread.currentThread()
                    .getContextClassLoader()).newInstance();
        } catch (Throwable t) {
            log.error("Cannot instantiate JSON SerDe :", t);
        }
        return null;
    }
}