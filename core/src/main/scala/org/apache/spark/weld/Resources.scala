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

import scala.collection.mutable
import scala.util.control.NonFatal

object Resources {
  def withCleanup[T](f: Resources => T): T = {
    val resources = new Resources
    try {
      val result = f(resources)
      resources.close()
      result
    } catch {
      case caught: Throwable =>
        // We want to throw and see the original exception so swallow exceptions during close.
        resources.close(swallowExceptions = true)
        throw caught
    }
  }
}

/**
 * Class that keeps track of resources.
 */
class Resources {
  private val closeables = mutable.Buffer.empty[AutoCloseable]

  /**
   * Add a closeable to the tracker.
   */
  def apply[T <: AutoCloseable](closeable: T): T = {
    closeables += closeable
    closeable
  }

  /**
   * Close all tracked resources. This function guarantees to call close on all tracked
   * resources, unless a fatal exception is thrown. If errors are encountered during the close
   * and `swallowExceptions` is set to `false`, the first [[Throwable]] will be stored and
   * others will be swallowed. If `swallowExceptions` is set to `true` all exceptions wil be
   * swallowed.
   */
  def close(swallowExceptions: Boolean = false): Unit = {
    var throwable: Throwable = null
    closeables.foreach { closeable =>
      try closeable.close() catch {
        case NonFatal(e) =>
          if (!swallowExceptions && throwable == null) {
            throwable = e
          }
      }
    }
    if (throwable != null) {
      throw throwable
    }
  }
}