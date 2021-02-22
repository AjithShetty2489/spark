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

class WeldError extends WeldManaged(WeldJNI.weld_error_new()) {
  override protected def doClose(): Unit = {
    WeldJNI.weld_error_free(handle)
  }

  override protected def cleaner = new WeldError.Cleaner(handle)

  def code: Int = {
    checkAccess()
    WeldJNI.weld_error_code(handle)
  }

  def message: String = {
    checkAccess()
    WeldJNI.weld_error_message(handle)
  }
}

object WeldError {
  private[weld] class Cleaner(handle: Long) extends Runnable {
    override def run(): Unit = WeldJNI.weld_error_free(handle)
  }
}

class WeldException(val code: Int, message: String) extends RuntimeException(message) {
  def this(message: String) = this(-1, message)
  def this(error: WeldError, code: Option[String] = None) =
    this(error.code, error.message + code.map("\n" + _).getOrElse(""))
}
