package com.flink.utils

import java.{lang => jl, util => ju}
import java.sql.ResultSet

import com.flink.utils.ScalaReflection._
import com.flink.utils.ScalaReflection.universe._

object JdbcUtils {

  def getProductDataCreateFunc[T <: Product: TypeTag](fieldColMap: Map[String, String] = Map.empty[String, String]): ResultSet => T = {
    val constructorMethodMirror = getConstructorConstructor[T]()
    val valueGetters = getConstructorParameters[T]().map { case (name, tpe) =>
      val col = fieldColMap.getOrElse(name, name)
      val valueGetter: ResultSet => Any = tpe match {
        case t if isSubtype(t, definitions.IntTpe) || isSubtype(t, localTypeOf[jl.Integer]) => { rst =>
          rst.getInt(col)
        }
        case t if isSubtype(t, definitions.LongTpe) || isSubtype(t, localTypeOf[jl.Long]) => { rst =>
          rst.getLong(col)
        }
        case t if isSubtype(t, definitions.DoubleTpe) || isSubtype(t, localTypeOf[jl.Double]) => { rst =>
          rst.getDouble(col)
        }
        case t if isSubtype(t, localTypeOf[String]) => { rst =>
          rst.getString(col)
        }
      }
      valueGetter
    }.toArray

    val func: ResultSet => T = rst => {
      val arr: Array[Any] = valueGetters.map{ getter =>
        var value = getter(rst)
        if (rst.wasNull){
          value = null
        }
        value
      }
      constructorMethodMirror(arr: _*).asInstanceOf[T]
    }

    func
  }

  def closeRst(rst: ResultSet): Unit = {
    if(rst != null){
      try {
        rst.close()
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

}
