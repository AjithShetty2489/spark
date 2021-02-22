/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.weld

class WeldContext private[weld](handle: Long) extends WeldManaged(handle) {
  override protected def doClose(): Unit = {
    WeldJNI.weld_context_free(handle)
  }

  /**
   * Obtain the memory usage of this context.
   */
  def getMemoryUsage: Long = {
    checkAccess()
    WeldJNI.weld_context_memory_usage(handle)
  }

  /**
   * Create a cleaner for the managed object.
   */
  override protected def cleaner = new WeldContext.Cleaner(handle)
}

object WeldContext {
  /**
   * Initialize a [[WeldContext]] using the default configuration.
   */
  def init(): WeldContext = {
    val conf = new WeldConf
    try init(conf) finally {
      conf.close()
    }
  }

  /**
   * Initialize a new a [[WeldContext]].
   */
  def init(conf: WeldConf): WeldContext = {
    val context = new WeldContext(WeldJNI.weld_context_new(conf.handle))
    // Returns NULL upon error.
    if (context.handle == 0) {
      val e = new WeldException("WeldContext initialization failed")
      throw e
    }
    context
  }

  private[weld] class Cleaner(handle: Long) extends Runnable {
    override def run(): Unit = WeldJNI.weld_context_free(handle)
  }
}
