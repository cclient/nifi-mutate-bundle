Nifi Mutate FlowFile Content Processor
================================

## 功能描述

适于于 flowfile为文本数据，一行一个json 'JSON-per-line' format

对每行一条json对象string的flowfile,按行应用mutate规则,修改原始json对象的值,输出变更后的flowfile

通常是根据需要因需实现语法解析规则

该项目主要是应用官方[expression-language](http://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html)（EL）实现mutate

## 应用原理

首先nifi官方 EL 默认可以应用在 'Variable Registry and FlowFile Attributes'上

查看源码和开发自定义Processor的经验可知应用 EL 方法为propertyValue.evaluateAttributeExpressions(flowFile)

evaluateAttributeExpressions有多个重载方法,其中一项为

`public PropertyValue evaluateAttributeExpressions(Map<String, String> attributes)`

查看nifi源码和实际验证知`evaluateAttributeExpressions(Map<String, String> attributes)`可以应用在任何Map<String, String>上

因此只要能将flowfile content转换为Map<String,String>,便可对flowfile content应用官方 EL

这点很容易,一行一条json对象string是比较常见的数据传输格式,例如(file,kafka,elasticsearch,logstash event等各种数据组件)

逐行读取flowfile,每行转为json对象后转换为Map<String,String>结构,应用官方 EL 取值,以一定的规则应用新值更改原始json后，把新的flowfile输出

---

目前转换规则封装的只是应用 EL 取出value后的行为,取value本身完全依赖官方EL

官方EL支持操作\'Variable Registry and FlowFile Attributes\',通过这种方式更改flowfile的content,增大了官方EL的应用场景

转换规则配置方式为json obj 目前只支持最外层key,a.b.c类的多级结构后续再支持

目标是实现部分logstash的常见功能[logstash-plugins-filters-mutate](https://www.elastic.co/guide/en/logstash/6.3/plugins-filters-mutate.html)

逐步替换logstash组件

EL执行后的值内容为PropertyValue，丢失了类型信息,因此取值后都需再指定一次targetType作类型转换,targetType参照[logstash-plugins-filters-mutate-convert](https://www.elastic.co/guide/en/logstash/6.3/plugins-filters-mutate.html#plugins-filters-mutate-convert)

目前支持targetType: boolean、string、integer、long、float、double，后期扩展其他类型

目前支持行为

* 1 delete key 删除key {"test_delete_key":{"operate":"remove"}}

* 2 add key 不存在该key则新增 {"test_value_sum":{"el":"${test_value_key1:plus(${test_value_key2}):plus(${test_value_key3})}","targetType":"long"}}

* 3 replace/update value 存在该key则更新 {"test_update_value": {"el": "2","targetType": "string"}}

* 4 rename key 更改key名称(实际为添加一个新key,并删除旧key) {"rename_key": {"el": "${test_rename_key}","targetType": "string","orginalKey": "test_rename_key","operate": "remove"}}

### demo

#### 原始json string

```org
{
    "test_delete_key": 1,
    "test_update_value": "1 to 2",
    "test_rename_key": "test_rename_key -> rename_key",
    "test_value_key1": 200,
    "test_value_key2": 200,
    "test_value_key3": 4,
    "test_value_toUpper": "cclient@hotmail.com",
    "test_value_timeStampToStr": "1590117918909"
}
```

#### 转换规则

```org
{
    "test_delete_key": {
        "operate": "remove"
    },
    "test_update_value": {
        "el": "2",
        "targetType": "string"
    },
    "rename_key": {
        "el": "${test_rename_key}",
        "targetType": "string",
        "orginalKey": "test_rename_key",
        "operate": "remove"
    },
    "test_value_toUpper": {
        "el": "${test_value_toUpper:toUpper()}",
        "targetType": "string"
    },
    "test_value_timeStampToStr": {
        "el": "${test_value_timeStampToStr:format(\"yyyy/MM/dd\", \"GMT\")}",
        "targetType": "string"
    },
    "test_value_sum": {
        "el": "${test_value_key1:plus(${test_value_key2}):plus(${test_value_key3})}",
        "targetType": "long"
    }
}
```

#### 变更后

```mutated
{
    "test_delete_key": 1,
    "test_update_value": "2",
    "test_value_key1": 200,
    "test_value_key2": 200,
    "test_value_key3": 4,
    "test_value_toUpper": "CCLIENT@HOTMAIL.COM",
    "test_value_timeStampToStr": "2020/05/22",
    "rename_key": "test_rename_key -> rename_key",
    "test_value_sum": 404
}
```

### 需求

数据处理会有对原始数据做微调的场景,过去通过大量logstash的filter实现

较为常用logstash filter有mutate,split,aggregate等，复杂场景通过写ruby代码和自定义filter解决

logstash很优秀,但随着更定制功能的需求,长期使用下也累积了一些使用上的痛点,通过logstash本身较难低成本的解决，调研学习nifi后，逐步将部分数据流迁移至nifi生态

最大的痛点

该Processor用来实现较为初级的数据微调需求

实际难度不高，主要创新点是把官方通常只应用在'Variable Registry and FlowFile Attributes'的EL，扩展应用在FlowFile Content上

项目只是展示可行性，实际可以更进一步的定制和添加功能

### deploy

#### 1 compile

`mvn package -Dmaven.test.skip=true`

#### 2 upload to one of

```nifi
nifi.nar.library.directory=./lib
nifi.nar.library.directory.custom=./lib_custom
nifi.nar.library.autoload.directory=./extensions
nifi.nar.working.directory=./work/nar/

```

cp nifi-mutate-nar/target/nifi-mutate-nar-0.1.nar nifi/lib_custom/

#### 3 restart nifi if need

nifi/bin/nifi.sh restart
