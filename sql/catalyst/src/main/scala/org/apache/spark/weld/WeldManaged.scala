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
 * Base class for classes for which an object exists that is managed by weld. All these classes
 * must be explicitly closed after use in order to prevent resource leaks.
 */
abstract class WeldManaged(val handle: Long) extends AutoCloseable {
  /**
   * Flag to indicate that the value is closed.
   */
  private var closed = false

  /**
   * Flag to indicate that the underlying reference will be cleaned up as soon as this object is
   * garbage collected.
   */
  private var autoClean = false

  /**
   * Close the weld managed object. Note that this method is idempotent.
   */
  override def close(): Unit = {
    if (!closed && !autoClean) {
      doClose()
      closed = true
    }
  }

  /**
   * Sets the closeable to auto cleaning mode. This means that the underlying handle is cleaned up
   * as soon as the object is garbage collected.
   */
  def markAutoCleanable(): Unit = {
    if (!closed && !autoClean) {
      autoClean = true
      Platform.registerForCleanUp(this, cleaner)
    }
  }

  /**
   * Close the weld managed object.
   */
  protected def doClose(): Unit

  /**
   * Create a cleaner for the managed object.
   */
  protected def cleaner: Runnable

  /**
   * Check if the weld managed is closed.
   */
  def isClosed(): Boolean = closed

  private[weld] def checkAccess() = {
    assert(!closed, "Cannot access an already closed object")
  }
}
