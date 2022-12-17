package com.flink.serialization

import com.flink.utils.JsonUtil
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

class JsonDataDeserializationSchema[T: TypeInformation](cls: Class[T], scData:Boolean = false) extends DeserializationSchema[T]{

  override def deserialize(message: Array[Byte]): T = {
    if(scData) JsonUtil.readScValue[T](message, cls) else JsonUtil.readValue[T](message, cls)
  }

  override def isEndOfStream(nextElement: T): Boolean = false

  override def getProducedType: TypeInformation[T] = implicitly[TypeInformation[T]]
}