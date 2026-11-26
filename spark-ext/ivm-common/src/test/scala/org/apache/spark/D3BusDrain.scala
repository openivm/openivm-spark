package org.apache.spark

object D3BusDrain {
  def apply(sc: SparkContext): Unit = sc.listenerBus.waitUntilEmpty(60000L)
}
