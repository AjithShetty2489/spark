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

case class Negate(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "-"
  override protected def withNewChild(newChild: Expr): Negate = copy(newChild)
}

case class Exp(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "exp"
  override protected def withNewChild(newChild: Expr): Exp = copy(newChild)
}

case class Log(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "log"
  override protected def withNewChild(newChild: Expr): Log = copy(newChild)
}

case class Sqrt(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "sqrt"
  override protected def withNewChild(newChild: Expr): Sqrt = copy(newChild)
}

case class Erf(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "erf"
  override protected def withNewChild(newChild: Expr): Erf = copy(newChild)
}

case class Sin(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "sin"
  override protected def withNewChild(newChild: Expr): Sin = copy(newChild)
}

case class Cos(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "cos"
  override protected def withNewChild(newChild: Expr): Cos = copy(newChild)
}

case class Tan(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "tan"
  override protected def withNewChild(newChild: Expr): Tan = copy(newChild)
}

case class ASin(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "asin"
  override protected def withNewChild(newChild: Expr): ASin = copy(newChild)
}

case class ACos(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "acos"
  override protected def withNewChild(newChild: Expr): ACos = copy(newChild)
}

case class ATan(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "atan"
  override protected def withNewChild(newChild: Expr): ATan = copy(newChild)
}

case class SinH(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "sinh"
  override protected def withNewChild(newChild: Expr): SinH = copy(newChild)
}

case class CosH(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "cosh"
  override protected def withNewChild(newChild: Expr): CosH = copy(newChild)
}

case class TanH(child: Expr) extends UnaryExpr with FunctionDesc {
  override def fn: String = "tanh"
  override protected def withNewChild(newChild: Expr): TanH = copy(newChild)
}
