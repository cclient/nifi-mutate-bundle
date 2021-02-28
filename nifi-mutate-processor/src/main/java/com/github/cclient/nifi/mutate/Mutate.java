/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.cclient.nifi.mutate;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.documentation.init.EmptyControllerServiceLookup;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @author cclient
 * flowfile 去重
 */
@Tags({"mutate"})
@WritesAttributes({})
@CapabilityDescription("读取flowfile,按行读取，对每一行json对象应用EL规则，修改json对象，将修改后的json对象写入新flowfile输出")
@RequiresInstanceClassLoading
public class Mutate extends AbstractProcessor {
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("SUCCESS")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FAILURE")
            .build();
    public static final PropertyDescriptor ConfJson = new PropertyDescriptor
            .Builder().name("ConfJson")
            .description("更改值的规则，根据官方的el组织的json字符串")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("{}")
            .build();
    protected ComponentLog log;
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ConfJson);
        this.descriptors = Collections.unmodifiableList(descriptors);
        log = getLogger();
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    /**
     * 实现json转map并应用配置修改
     * 官方操作json使用的是com.fasterxml.jackson，个人更习惯用com.alibaba.fastjson，同时保留了两种操作方式
     * @param orginalJsonStr
     * @param confJson
     * @param isFastJson 是否使用fastjson
     * @return
     * @throws IOException
     */
    public String mutate(String orginalJsonStr, JSONObject confJson, boolean isFastJson) throws IOException {
        if (isFastJson) {
            JSONObject orgnialJO = JSONObject.parseObject(orginalJsonStr);
            return mutateByFastjson(orgnialJO, confJson).toJSONString();
        } else {
            final ObjectMapper mapper = new ObjectMapper();
            final AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);
            rootNodeRef.set(mapper.readTree(orginalJsonStr));
            JsonNode newJN = mutateByJackson(rootNodeRef.get(), confJson);
            return newJN.toString();
        }
    }

    public JsonNode mutateByJackson(JsonNode orginalJsonNode, JSONObject confJson) {
        final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
        ObjectNode mutatedJsonObj = new ObjectNode(nodeFactory);
        ControllerServiceLookup serviceLookup = new EmptyControllerServiceLookup();
        Map<String, String> orginalMap = new HashMap<>(orginalJsonNode.size());
        orginalJsonNode.fields().forEachRemaining(field -> {
            mutatedJsonObj.put(field.getKey(), field.getValue());
            JsonNode jv = field.getValue();
            String value;
            if (jv.isTextual()) {
                value = jv.asText();
            } else {
                value = jv.toString();
            }
            orginalMap.put(field.getKey(), value);
        });
        for (Map.Entry<String, Object> stringObjectEntry : confJson.entrySet()) {
            String targetKey = stringObjectEntry.getKey();
            JSONObject confValue = confJson.getJSONObject(targetKey);
            String elStr = confValue.getString("el");
            String targetType = confValue.getOrDefault("targetType", "string").toString();
            String upsert = confValue.getOrDefault("operate", "upsert").toString();
            if ("remove".equals(upsert) && !confValue.containsKey("orginalKey")) {
                mutatedJsonObj.remove(targetKey);
                continue;
            }
            StandardPropertyValue propertyValue = new StandardPropertyValue(elStr, serviceLookup, ParameterLookup.EMPTY);
            PropertyValue propertyValue1 = propertyValue.evaluateAttributeExpressions(orginalMap);
            switch (targetType) {
                case "integer":
                    mutatedJsonObj.put(targetKey, propertyValue1.asInteger());
                    break;
                case "boolean":
                    mutatedJsonObj.put(targetKey, propertyValue1.asBoolean());
                    break;
                case "double":
                    mutatedJsonObj.put(targetKey, propertyValue1.asDouble());
                    break;
                case "float":
                    mutatedJsonObj.put(targetKey, propertyValue1.asFloat());
                    break;
                case "long":
                    mutatedJsonObj.put(targetKey, propertyValue1.asLong());
                    break;
                default:
                    mutatedJsonObj.put(targetKey, propertyValue1.getValue());
            }
            if ("remove".equals(upsert)) {
                String orginalKey = confValue.getString("orginalKey");
                mutatedJsonObj.remove(orginalKey);
            }
        }
        return mutatedJsonObj;
    }

    public JSONObject mutateByFastjson(JSONObject orginalJsonNode, JSONObject confJson) {
        JSONObject mutatedJsonObj = new JSONObject();
        ControllerServiceLookup serviceLookup = new EmptyControllerServiceLookup();
        Map<String, String> orginalMap = new HashMap<>(orginalJsonNode.size());
        orginalJsonNode.entrySet().forEach(field -> {
            mutatedJsonObj.put(field.getKey(), field.getValue());
            Object jv = field.getValue();
            String value;
            if (jv instanceof String) {
                value = (String) jv;
            } else {
                value = jv.toString();
            }
            orginalMap.put(field.getKey(), value);
        });
        for (Map.Entry<String, Object> stringObjectEntry : confJson.entrySet()) {
            String targetKey = stringObjectEntry.getKey();
            JSONObject confValue = confJson.getJSONObject(targetKey);
            String elStr = confValue.getString("el");
            String targetType = confValue.getOrDefault("targetType", "string").toString();
            String upsert = confValue.getOrDefault("operate", "upsert").toString();
            if ("remove".equals(upsert) && !confValue.containsKey("orginalKey")) {
                mutatedJsonObj.remove(targetKey);
                continue;
            }
            StandardPropertyValue propertyValue = new StandardPropertyValue(elStr, serviceLookup, ParameterLookup.EMPTY);
            PropertyValue propertyValue1 = propertyValue.evaluateAttributeExpressions(orginalMap);
            switch (targetType) {
                case "integer":
                    mutatedJsonObj.put(targetKey, propertyValue1.asInteger());
                    break;
                case "boolean":
                    mutatedJsonObj.put(targetKey, propertyValue1.asBoolean());
                    break;
                case "double":
                    mutatedJsonObj.put(targetKey, propertyValue1.asDouble());
                    break;
                case "float":
                    mutatedJsonObj.put(targetKey, propertyValue1.asFloat());
                    break;
                case "long":
                    mutatedJsonObj.put(targetKey, propertyValue1.asLong());
                    break;
                default:
                    mutatedJsonObj.put(targetKey, propertyValue1.getValue());
            }
            if ("remove".equals(upsert)) {
                String orginalKey = confValue.getString("orginalKey");
                mutatedJsonObj.remove(orginalKey);
            }
        }
        return mutatedJsonObj;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        String confJsonStr = processContext.getProperty(ConfJson).getValue();
        JSONObject confJson = JSONObject.parseObject(confJsonStr);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);
        InputStream inputStream = new ByteArrayInputStream(baos.toByteArray());
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        final ByteArrayOutputStream baosNext = new ByteArrayOutputStream();
        boolean isError = false;
        try {
            while (reader.ready()) {
                String line = reader.readLine();

                String mutatedJsonStr= mutate(line,confJson,true);
                IOUtils.write(mutatedJsonStr + "\n", baosNext, Charset.forName("UTF-8"));
            }
        } catch (IOException e) {
            isError = true;
            log.error("munate error", e);
            e.printStackTrace();
        }
        if (isError) {
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        FlowFile flowFileNext = session.create(flowFile);
        flowFileNext = session.write(flowFileNext, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                out.write(baosNext.toByteArray());
            }
        });
        session.transfer(flowFileNext, REL_SUCCESS);
        session.remove(flowFile);
    }
}
