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
/**
 * A wrapper around the weld configuration object.
 *
 * Note that this object must be closed after usage.
 */
class WeldConf extends WeldManaged(WeldJNI.weld_conf_new()) {
  override protected def doClose(): Unit = {
    WeldJNI.weld_conf_free(handle)
  }

  override protected def cleaner = new WeldConf.Cleaner(handle)

  def get(key: String): String = {
    checkAccess()
    WeldJNI.weld_conf_get(handle, key)
  }

  def set(key: String, value: String): Unit = {
    checkAccess()
    WeldJNI.weld_conf_set(handle, key, value)
  }
}

object WeldConf {
  private[weld] class Cleaner(handle: Long) extends Runnable {
    override def run(): Unit = WeldJNI.weld_conf_free(handle)
  }
}
