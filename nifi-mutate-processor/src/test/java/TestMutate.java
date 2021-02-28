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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.github.cclient.nifi.mutate.Mutate;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestMutate {

    private TestRunner runner;

    public String jsonPretty(String jsonStr){
        JSONObject object = JSONObject.parseObject(jsonStr);
        String pretty = JSON.toJSONString(object, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
                SerializerFeature.WriteDateUseDateFormat);
        return pretty;
    }

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(Mutate.class);
        String jsonLine = "{\"test_delete_key\":1,\"test_update_value\":\"1 to 2\",\"test_rename_key\":\"test_rename_key -> rename_key\",\"test_value_key1\":200,\"test_value_key2\":200,\"test_value_key3\":4,\"test_value_toUpper\":\"cclient@hotmail.com\",\"test_value_timeStampToStr\":\"1590117918909\"}";
        System.out.println("--- orginal json string ---");
        System.out.println(jsonPretty(jsonLine));
        String jsonConf = "{\"test_delete_key\":{\"operate\":\"remove\"},\"test_update_value\":{\"el\":\"2\",\"targetType\":\"string\"},\"rename_key\":{\"el\":\"${test_rename_key}\",\"targetType\":\"string\",\"orginalKey\":\"test_rename_key\",\"operate\":\"remove\"},\"test_value_toUpper\":{\"el\":\"${test_value_toUpper:toUpper()}\",\"targetType\":\"string\"},\"test_value_timeStampToStr\":{\"el\":\"${test_value_timeStampToStr:format(\\\"yyyy/MM/dd\\\", \\\"GMT\\\")}\",\"targetType\":\"string\"},\"test_value_sum\":{\"el\":\"${test_value_key1:plus(${test_value_key2}):plus(${test_value_key3})}\",\"targetType\":\"long\"}}";
        runner.enqueue(jsonLine);
        System.out.println("--- mutate config ---");
        System.out.println(jsonPretty(jsonConf));
        runner.setProperty(Mutate.ConfJson, jsonConf);
    }

    @Test
    public void testProcessor() {
        runner.run();
        List<MockFlowFile> result = runner.getFlowFilesForRelationship(Mutate.REL_SUCCESS);
        System.out.println(result.size());
        System.out.println("--- mutated json string ---");
        System.out.println(jsonPretty(new String(result.get(0).toByteArray())));
        Assert.assertEquals(1, result.size());
    }

}
