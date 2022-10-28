package com.flink.utils

import java.util.Locale

/**
 * key不区分大小写的map
 * 构建key不区分大小写的map。对于key需要区分大小写的情况，可以访问原始输入映射originalMap。
 * 主构造函数被标记为private以避免嵌套的创建此类，那样原始的map的key也将变的不区分大小写。
 * 注意：CaseInsensitiveMap是可序列化的。但是，在转换后，例如filterKeys，它可能会变得不可序列化。
 *
 * Builds a map in which keys are case insensitive. Input map can be accessed for cases where
 * case-sensitive information is required. The primary constructor is marked private to avoid
 * nested case-insensitive map creation, otherwise the keys in the original map will become
 * case-insensitive in this scenario.
 * Note: CaseInsensitiveMap is serializable. However, after transformation, e.g. `filterKeys()`,
 *       it may become not serializable.
 */
class CaseInsensitiveMap[T] private (val originalMap: Map[String, T]) extends Map[String, T]
  with Serializable {
  val keyLowerCasedMap = originalMap.map(kv => kv.copy(_1 = kv._1.toLowerCase(Locale.ROOT)))

  override def get(k: String): Option[T] = keyLowerCasedMap.get(k.toLowerCase(Locale.ROOT))

  override def contains(k: String): Boolean =
    keyLowerCasedMap.contains(k.toLowerCase(Locale.ROOT))

  override def +[B1 >: T](kv: (String, B1)): CaseInsensitiveMap[B1] = {
    new CaseInsensitiveMap(originalMap.filter(!_._1.equalsIgnoreCase(kv._1)) + kv)
  }

  def ++(xs: TraversableOnce[(String, T)]): CaseInsensitiveMap[T] = {
    xs.foldLeft(this)(_ + _)
  }

  override def iterator: Iterator[(String, T)] = keyLowerCasedMap.iterator

  override def -(key: String): Map[String, T] = {
    new CaseInsensitiveMap(originalMap.filter(!_._1.equalsIgnoreCase(key)))
  }

  def toMap: Map[String, T] = originalMap
}

object CaseInsensitiveMap {
  def apply[T](params: Map[String, T]): CaseInsensitiveMap[T] = params match {
    case caseSensitiveMap: CaseInsensitiveMap[T] => caseSensitiveMap
    case _ => new CaseInsensitiveMap(params)
  }
}
