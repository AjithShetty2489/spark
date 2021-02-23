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

class WeldModule private(handle: Long) extends WeldManaged(handle) {
  override protected def doClose(): Unit = {
    WeldJNI.weld_module_free(handle)
  }

  /**
   * Run the module with default parameters.
   */
  def run(input: WeldValue): WeldValue = {
    val context = WeldContext.init()
    try run(context, input) finally {
      context.close()
    }
  }

  /**
   * Run the module.
   */
  def run(context: WeldContext, input: WeldValue): WeldValue = {
    checkAccess()
    val error = new WeldError
    val output = new WeldValue(
      WeldJNI.weld_module_run(
        handle,
        context.handle,
        input.handle,
        error.handle),
      size = -1L)
    if (error.code != 0) {
      val e = new WeldException(error)
      error.close()
      output.close()
      throw e
    }
    output
  }

  /**
   * Create a cleaner for the managed object.
   */
  override protected def cleaner = new WeldModule.Cleaner(handle)
}

object WeldModule {
  /**
   * Compile the given code into a [[WeldModule]] using the default configuration.
   */
  def compile(code: String): WeldModule = {
    val conf = new WeldConf
    try compile(conf, code) finally {
      conf.close()
    }
  }

  /**
   * Compile the given code into a [[WeldModule]].
   */
  def compile(conf: WeldConf, code: String): WeldModule = {
    val error = new WeldError
    val module = new WeldModule(WeldJNI.weld_module_compile(code, conf.handle, error.handle))
    if (error.code != 0) {
      val e = new WeldException(error, Some(code))
      error.close()
      module.close()
      throw e
    }
    module
  }

  private[weld] class Cleaner(handle: Long) extends Runnable {
    override def run(): Unit = WeldJNI.weld_module_free(handle)
  }
}
