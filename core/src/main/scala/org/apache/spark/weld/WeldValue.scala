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

import java.nio.{ByteBuffer, ByteOrder}

import scala.annotation.varargs

/**
 * This is a wrapper around the weld value object.
 *
 * Note that this object must be closed after usage.
 */
class WeldValue private[weld](handle: Long, val size: Long = -1) extends WeldManaged(handle) {
  override protected def doClose(): Unit = WeldJNI.weld_value_free(handle)

  override protected def cleaner = new WeldValue.Cleaner(handle)

  /**
   * Get the address to the data this value encapsulates.
   */
  def getPointer: Long = {
    checkAccess()
    WeldJNI.weld_value_pointer(handle)
  }

  /**
   * Get the weld run ID this value is associated with.
   */
  def getRunId: Long = {
    checkAccess()
    WeldJNI.weld_value_run(handle)
  }


  /**
   * Get the context of this value.
   *
   * The context's reference count is incremented when this method is called,
   * and should thus be freed using `weld_context_free`.
   */
  def getContext: WeldContext = {
    checkAccess()
    new WeldContext(WeldJNI.weld_value_context(handle))
  }

  /**
   * Get the result of a weld module run.
   */
  def result(structType: StructType): WeldStruct = {
    checkAccess()
    new WeldStruct(getPointer, structType)
  }

  /**
   * Get the result of a weld module run.
   */
  @varargs
  def result(fieldTypes: WeldType*): WeldStruct = result(StructType(fieldTypes))
}

object WeldValue {
  private def directByteBuffer(size: Int) = {
    ByteBuffer.allocateDirect(size).order(ByteOrder.nativeOrder)
  }

  /**
   * Create an empty [[WeldValue]]
   */
  def empty(): WeldValue = apply(directByteBuffer(0))

  /**
   * Create a [[WeldValue]] using a (direct) [[ByteBuffer]].
   */
  def apply(buffer: ByteBuffer): WeldValue = {
    val direct = if (!buffer.isDirect) {
      val replacement = directByteBuffer(buffer.limit())
      replacement.put(buffer)
    } else {
      buffer
    }
    apply(WeldJNI.weld_get_buffer_pointer(direct), buffer.limit())
  }

  /**
   * Create a [[WeldValue]] from the given pointer.
   */
  def apply(pointer: Long): WeldValue = apply(pointer, -1L)

  /**
   * Create a sized [[WeldValue]] from the given pointer.
   */
  def apply(pointer: Long, size: Long): WeldValue = {
    new WeldValue(WeldJNI.weld_value_new(pointer), size)
  }

  private[weld] class Cleaner(handle: Long) extends Runnable {
    override def run(): Unit = WeldJNI.weld_value_free(handle)
  }
}
