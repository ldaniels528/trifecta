package com.ldaniels528.verify.vscript

import com.ldaniels528.tabular.Tabular

import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, future}
import scala.util.{Failure, Success, Try}

/**
 * VScript Runtime
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object VScriptRuntime {
  private[this] val logger = org.apache.log4j.Logger.getLogger(getClass)
  private[this] val nativeOps = collection.concurrent.TrieMap[String, OpCode]()
  private[this] val tabular = new Tabular()
  private[vscript] val CLASS_EXT = ".qsc"

  private def asNamedEntity(entity: OpCode)(implicit scope: Scope): NamedEntity = {
    entity.eval match {
      case Some(ne: NamedEntity) => ne
      case _ =>
        throw new IllegalArgumentException(s"Object '$entity' could not resolved")
    }
  }

  private def asNumber(value: Any): Double = {
    normalizeValue(value) match {
      case s: String => s.toDouble
      case v: Double => v
      case v: Int => v.toDouble
      case x => throw new IllegalArgumentException(s"Invalid numeric value - '$x' [${x.getClass.getName}]")
    }
  }

  private def asSeqString[A](seq: Seq[A]) = {
    val s = seq map normalizeValue
    if (Tabular.isPrimitives(s)) s"[${s map asString mkString ", "}]" else tabular.transform(s) mkString "\n"
  }

  private[vscript] def asString(value: Any): String = value match {
    // collections & monads
    case a: Array[_] => asSeqString(a)
    case e: Either[_, _] => asString(if (e.isLeft) e.left else e.right)
    case f: Future[_] => asString(Await.result(f, 30 seconds))
    case l: List[_] => asSeqString(l)
    case o: Option[_] => asString(o getOrElse "")
    case s: Seq[_] => asSeqString(s)
    case s: Set[_] => asSeqString(s.toSeq)
    case t: Try[_] => t match {
      case Success(v) => asString(v)
      case Failure(e) => e.getMessage
    }

    // primitives
    case d: Double => (if (d == d.toInt) d.toInt else d).toString
    case f: Float => (if (f == f.toInt) f.toInt else f).toString
    case i: Int => i.toString
    case l: Long => l.toString
    case n: Number => (if (n.doubleValue == n.intValue) n.intValue else n.doubleValue).toString
    case s: Short => s.toString
    case s: String => s

    // objects
    case x => asSeqString(Seq(x))
  }

  private def createScopeWithArgs(name: String, params: Seq[String], args: Seq[OpCode], parentScope: Option[Scope]): Scope = {
    // the parameter count must match!
    if (params.length != args.length)
      throw new IllegalStateException(s"Argument mismatch for function '$name'")

    // create a new scope with the parameters as variables
    val objScope = new CascadingScope(parentScope)
    params zip args foreach {
      case (k, v) =>
        objScope += Variable(k, ConstantValue(v.eval(objScope)))
    }
    objScope
  }

  private def isTrue(v: Any): Boolean = v match {
    case b: Boolean => b
    case o: Option[_] => if (o.isDefined) isTrue(o.get) else false
    case x =>
      logger.info(s"isTrue: unexpected result = '$x'")
      false
  }

  private[vscript] def loadNativeClass(className: String): OpCode = {
    try {
      Class.forName(className).newInstance() match {
        case o: OpCode => o
        case _ =>
          throw new IllegalArgumentException(s"Class '$className' is not a subclass of ${classOf[OpCode].getName}")
      }
    } catch {
      case e: ClassNotFoundException =>
        throw new IllegalStateException(s"Native class '$className' could not be resolved")
      case e: Throwable =>
        throw new IllegalStateException(s"Unexpected error loading native class '$className': ${e.getMessage}")
    }
  }

  private def normalizeValue(value: Any): Any = {
    // if the value is a future, wait for it and resubmit
    value match {
      case e: Either[_, _] => normalizeValue(if (e.isLeft) e.left else e.right)
      case f: Future[_] => normalizeValue(Await.result(f, 30 seconds))
      case o: Option[_] => normalizeValue(o getOrElse "")
      case t: Try[_] => t match {
        case Success(s) => normalizeValue(s)
        case Failure(e) => throw new IllegalStateException(e.getMessage, e)
      }
      case x => x
    }
  }

  private def resolve(value: Any)(implicit scope: Scope): Any = {
    val normValue = normalizeValue(value)
    //logger.info(s"resolve: normValue = $normValue [${normValue.getClass.getName}]")
    normValue match {
      case Variable(_, v) => resolve(v.eval)
      case ner: NamedEntityRef =>
        val result = ner.eval
        if (result.nonEmpty) resolve(result)
        else throw new IllegalArgumentException(s"Object '${ner.name}' could not resolved")
      case o: OpCode => resolve(o.eval)
      case v => v
    }
  }

  /**
   * Represents an array index reference
   */
  case class ArrayIndexRef(entity: OpCode, index: OpCode) extends OpCode {
    override def toString = s"$entity[$index]"

    override def eval(implicit scope: Scope) = {
      val idx = asNumber(resolve(index)).toInt
      resolve(entity) match {
        case s: String => Some(s(idx))
        case s: Seq[_] => Some(s(idx))
        case a: Array[_] => Some(a(idx))
        case _ => throw new IllegalArgumentException(s"Cannot reference index $idx")
      }
    }
  }

  /**
   * Represents an array value
   */
  case class ArrayValue(elems: Seq[OpCode]) extends OpCode {
    override def eval(implicit scope: Scope) = Some(elems map (_.eval getOrElse "") toList)

    override def toString = s"[ ${elems map (_.toString) mkString ", "} ]"
  }

  /**
   * Represents an asynchronous operation
   */
  case class AsyncOp(opCode: OpCode) extends OpCode {
    override def eval(implicit scope: Scope) = Some(future(opCode.eval))

    override def toString = s"async $opCode"
  }

  /**
   * Represents a code block; returns the last evaluated expression
   */
  case class CodeBlock(expressions: Seq[OpCode]) extends OpCode {
    override def toString = s"{ ${expressions mkString "; "} }"

    override def eval(implicit scope: Scope) = {
      if (expressions.isEmpty) None
      else {
        val myScope = new CascadingScope(Some(scope))
        var last: Option[Any] = None
        expressions foreach (fx => last = fx.eval(myScope))
        last
      }
    }
  }

  /**
   * Represents a constant value
   */
  case class ConstantValue(value: Option[Any]) extends OpCode {
    override def eval(implicit scope: Scope) = value

    override def toString = value match {
      case Some(s: String) => s""""$s""""
      case Some(x) => asString(x)
      case None => ""
    }
  }

  /**
   * Represents a new class definition
   */
  case class ClassNew(className: String, params: Seq[String], code: OpCode) extends OpCode {
    override def toString = s"class $className $code"

    override def eval(implicit scope: Scope) = {
      val classDef = ClassDef(className, params, code)
      scope += classDef
      Some(classDef)
    }
  }

  /**
   * Represents a decrement variable operation
   */
  case class Decr(entity: OpCode) extends OpCode {
    override def toString = s"$entity--"

    override def eval(implicit scope: Scope) = {
      asNamedEntity(entity) match {
        case v@Variable(name, value) =>
          val result = Some(asNumber(normalizeValue(value.eval)) - 1)
          v.value = ConstantValue(result)
          result
        case _ => throw new IllegalArgumentException(s"Object '$entity' is not a variable")
      }
    }
  }

  /**
   * Represents an for-each expression
   */
  case class ForEach(varName: String, collection: OpCode, code: OpCode) extends OpCode {
    override def toString = s"foreach $varName ($collection) $code"

    override def eval(implicit scope: Scope) = {
      val innerScope = new CascadingScope(Some(scope))
      normalizeValue(resolve(collection)) match {
        case s: String => s foreach { ca =>
          innerScope += Variable(varName, ConstantValue(Some(String.valueOf(ca))))
          code.eval(innerScope)
        }
        case l: List[_] => l foreach { item =>
          innerScope += Variable(varName, ConstantValue(Some(item)))
          code.eval(innerScope)
        }
        case x =>
          logger.info(s"x => '$x' (${x.getClass.getSimpleName})")
          throw new IllegalArgumentException(s"Cannot iterate $x")
      }
      None
    }
  }

  /**
   * Represents an for expression
   */
  case class ForLoop(initial: OpCode, condition: OpCode, code: OpCode) extends OpCode {
    override def toString = s"for($initial, $condition) $code"

    override def eval(implicit scope: Scope) = {
      var last: Option[Any] = initial.eval
      while (condition.eval exists isTrue) {
        last = code.eval
      }
      last
    }
  }

  /**
   * Represents a call-by-name function reference
   */
  case class FunctionCall(entity: OpCode, args: Seq[OpCode]) extends OpCode {
    override def toString = s"$entity(${args mkString ", "})"

    override def eval(implicit scope: Scope) = {
      // resolve the function
      resolve(entity) match {
        case Function(name, params, expression) =>
          expression.eval(createScopeWithArgs(name, params, args, Some(scope)))
        case _ =>
          throw new IllegalArgumentException(s"Object $entity is not a function")
      }
    }
  }

  /**
   * Represents a function definition construct
   */
  case class FunctionNew(name: String, params: Seq[String], code: OpCode) extends OpCode {
    override def toString = s"def $name(${params mkString ", "}) $code"

    override def eval(implicit scope: Scope) = {
      val fx = Function(name, params, code)
      scope += fx
      Some(fx)
    }
  }

  /**
   * Represents an if-else expression
   */
  case class IfElse(condition: OpCode, positive: OpCode, negative: OpCode) extends OpCode {
    override def toString = s"if $condition $positive ${if (negative != NoOp) s"else $negative"}"

    override def eval(implicit scope: Scope) = {
      val yes = condition.eval exists isTrue
      if (yes) positive.eval else negative.eval
    }
  }

  /**
   * Represents an import function
   * Syntax: import <className>
   */
  case class Import(value: OpCode) extends OpCode {
    override def toString = s"import $value"

    override def eval(implicit scope: Scope) = {
      val classes = normalizeValue(value.eval) match {
        case className: String => List(loadClass(className))
        case classNames: List[_] => classNames flatMap {
          case className: String => Some(loadClass(className))
          case _ => None
        }
        case _ => Nil
      }
      logger.info(s"classes = $classes")
      classes foreach (_.eval(scope))
      None
    }

    private def loadClass(className: String): OpCode = {
      VScriptByteCodeUtil.loadClass(new java.io.File(s"$className$CLASS_EXT"))
    }
  }

  /**
   * Represents a increment variable operation
   */
  case class Incr(entity: OpCode) extends OpCode {
    override def toString = s"$entity++"

    override def eval(implicit scope: Scope) = {
      asNamedEntity(entity) match {
        case v@Variable(name, value) =>
          val result = Some(asNumber(normalizeValue(value.eval)) + 1)
          v.value = ConstantValue(result)
          result
        case value =>
          logger.info(s"entity '$entity' [${entity.getClass.getSimpleName}], value = '$value'")
          throw new IllegalArgumentException(s"Object '$entity' is not a variable")
      }
    }
  }

  /**
   * Represents a entity-by-name reference (class, function or variable)
   */
  case class NamedEntityRef(name: String) extends OpCode {
    override def toString = name

    override def eval(implicit scope: Scope): Option[NamedEntity] = scope.getNamedEntity(name)
  }

  /**
   * Represents a entity-by-name reference (class, function or variable)
   */
  case class NativeOpCode(classNameRef: OpCode) extends OpCode {
    override def toString = s"native($classNameRef)"

    override def eval(implicit scope: Scope) = {
      classNameRef.eval match {
        case Some(className: String) =>
          nativeOps.get(className) match {
            case Some(opCode) => opCode.eval
            case None =>
              val opCode = loadNativeClass(className)
              nativeOps += (className -> opCode)
              opCode.eval
          }
        case _ =>
          throw new IllegalArgumentException("String value required for native code class name")
      }
    }
  }

  /**
   * Represents a non-operational instruction
   */
  case object NoOp extends OpCode {
    override def eval(implicit scope: Scope) = None

    override def toString = "{}"
  }

  /**
   * Represents an object-field-by-name reference
   */
  case class ObjectFieldRef(entity: OpCode, field: String) extends OpCode {
    override def toString = s"$entity.$field"

    override def eval(implicit scope: Scope) = {
      val bean = resolve(entity)

      try {
        val m = bean.getClass.getMethod(field)
        m.invoke(bean) match {
          case o: Option[_] => o
          case v => Some(v)
        }
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to retrieve ${bean.getClass.getSimpleName}#$field", e)
          None
      }
    }
  }

  /**
   * Represents an object instance
   */
  case class ObjectInst(className: String, scope: Scope) extends OpCode {
    override def eval(implicit scope: Scope) = None

    override def toString = s"$className<classes=${scope.getClassDefs.length}, functions=${scope.getFunctions.length}, variables=${scope.getVariables.length}>"
  }

  /**
   * Represents a object method-by-name reference
   * Syntax: Quotes::getQuote("AAPL")
   */
  case class ObjectMethodRef(parent: OpCode, child: OpCode) extends OpCode {
    override def toString = s"$parent::$child"

    override def eval(implicit scope: Scope) = {
      resolve(parent) match {
        case ObjectInst(_, objScope) => child.eval(objScope)
        case x =>
          logger.info(s"ObjectMethodRef: scope = '$scope' [${scope.getClass.getName}]")
          throw new IllegalArgumentException(s"'$parent' is not an object")
      }
    }
  }

  /**
   * Represents the instantiation of a new object
   */
  case class ObjectNew(className: String, args: Seq[OpCode]) extends OpCode {
    override def toString = s"new $className(${args mkString ", "})"

    override def eval(implicit scope: Scope) = {
      // resolve the class
      scope.getNamedEntity(className) match {
        case Some(ClassDef(name, params, constructor)) =>
          // create the object scope, and evaluate the constructor
          val objScope = createScopeWithArgs(name, params, args, None)
          constructor match {
            case CodeBlock(ops) => ops foreach (_.eval(objScope))
            case op => op.eval(objScope)
          }

          // return the object instance
          Some(ObjectInst(className, objScope))
        case _ =>
          throw new IllegalArgumentException(s"Object $className is not a class")
      }
    }
  }

  /**
   * Represents an expression
   */
  case class Expression(operator: String, lvalue: OpCode, rvalue: OpCode) extends OpCode {
    override def toString = s"($lvalue $operator $rvalue)"

    override def eval(implicit scope: Scope) = {
      val (v0, v1) = (resolve(lvalue), resolve(rvalue))
      val result = operator match {
        case ":+" => append(v0, v1)
        case "+:" => prepend(v0, v1)
        case "==" => compare(v0, v1)
        case "===" => v0 == v1
        case _ => compute(v0, v1)
      }
      Some(result)
    }

    private def append(v0: Any, v1: Any) = {
      v0 match {
        case l: List[_] => l :+ v1
        case s: String => s + v1
        case x => x.toString + v1
      }
    }

    private def prepend(v0: Any, v1: Any) = {
      v0 match {
        case l: List[_] => v1 :: l
        case s: String => v1.toString + s
        case x => v1.toString + x
      }
    }

    private def compare(v0: Any, v1: Any): Boolean = v0 == v1

    private def compute(v0: Any, v1: Any): Double = {
      val (n0, n1) = (asNumber(v0), asNumber(v1))
      operator match {
        case "+" => n0 + n1
        case "-" => n0 - n1
        case "*" => n0 * n1
        case "/" => n0 / n1
        case "%" => n0 % n1
        case x => throw new IllegalArgumentException(s"Invalid operator - '$x'")
      }
    }
  }

  /**
   * Represents a print function
   */
  case class Print(value: OpCode) extends OpCode {
    override def toString = s"println $value"

    override def eval(implicit scope: Scope) = {
      print(asString(value.eval))
      None
    }
  }

  /**
   * Represents a print-with-new-line function
   */
  case class Println(value: OpCode) extends OpCode {
    override def toString = s"println $value"

    override def eval(implicit scope: Scope) = {
      println(asString(value.eval))
      None
    }
  }

  /**
   * Represents a variable definition construct
   */
  case class VariableNew(name: String, value: OpCode) extends OpCode {
    override def toString = s"val $name = $value"

    override def eval(implicit scope: Scope) = {
      val result = value.eval
      scope += Variable(name, ConstantValue(result))
      result
    }
  }

  /**
   * Represents a while .. do expression
   */
  case class WhileDo(condition: OpCode, code: OpCode) extends OpCode {
    override def toString = s"while ($condition) $code"

    override def eval(implicit scope: Scope) = {
      var last: Option[Any] = None
      while (condition.eval exists isTrue) {
        last = code.eval
      }
      last
    }
  }

}

