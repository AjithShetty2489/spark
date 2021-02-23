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

package org.apache.spark.weld.expressions

trait DescBuilder {
  def builder: StringBuilder

  def appendPrefix(p: String): DescBuilder = {
    newBuilder(p.length).append(p)
  }

  def append(b: String, sep: String, e: String, exprs: Seq[ExprLike]): DescBuilder = {
    val newBuilder = appendPrefix(b)
    var first = true
    exprs.foreach { expr =>
      if (!first) {
        newBuilder.append(sep)
      }
      newBuilder.append(expr)
      first = false
    }
    newBuilder.append(e)
  }

  def append(e: ExprLike): DescBuilder = {
    e.buildDesc(this)
    this
  }

  def append(s: String): DescBuilder = {
    builder.append(s)
    this
  }

  def newLine(): DescBuilder

  def newBuilder(increment: Int): DescBuilder

  def desc: String = builder.toString()
}


case class IndentedDescBuilder(builder: StringBuilder = new StringBuilder, indent: Int = 0) extends DescBuilder {
  override def newBuilder(increment: Int): DescBuilder = copy(indent = indent + increment)
  override def newLine(): DescBuilder = append("\n").append(" " * indent)
}

case class SimpleDescBuilder(builder: StringBuilder = new StringBuilder) extends DescBuilder {
  override def newBuilder(increment: Int): DescBuilder = this
  override def newLine(): DescBuilder = append(" ")
}
