package com.ldaniels528.verify.vscript

import java.io.{ByteArrayOutputStream, File, FileInputStream}
import java.nio.ByteBuffer

import com.ldaniels528.verify.util.VerifyUtils._
import com.ldaniels528.verify.vscript.VScriptRuntime._
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

/**
 * VScript ByteCode Generator
 * @author lawrence.daniels@gmail.com
 */
object VScriptByteCodeUtil {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val VS_FILE_ID: Int = 0xDECAFFED
  private[this] val MAJOR_VERSION: Byte = 0x01
  private[this] val MINOR_VERSION: Byte = 0x01

  /**
   * Compiles the given instruction into byte code
   */
  def compileByteCode(op: OpCode): Array[Byte] = {
    val binary = VSBinary()
    binary += encode(op)(binary)
    binary.toArray
  }

  /**
   * Decompiles the given byte code into an instruction
   */
  def decompileByteCode(bytes: Array[Byte]): OpCode = decode(VSBinary.load(bytes))

  /**
   * Decompiles the given binary into assembly code
   */
  def unassembleByteCode(qs: VSBinary): List[AssyCode] = unassemble(qs)

  /**
   * Decompiles the given byte code into assembly code
   */
  def unassembleByteCode(bytes: Array[Byte]): List[AssyCode] = unassemble(VSBinary.load(bytes))

  /**
   * Displays the given bytes in a human friendly way
   */
  def dump(bytes: Array[Byte], width: Int = 20) = {
    // define the ASCII and Hex display mappings
    def hex(b: Byte): String = "%02x".format(b)
    def ascii(b: Byte): Char = if (b >= 32 && b <= 128) b.toChar else '.'

    // build the output
    val sets = {
      (bytes sliding(width, width)).toSeq
    }
    val grid = sets map (seq => (seq map hex mkString " ", seq map ascii mkString ""))
    grid foreach {
      case (code, data) => println(s"%-${width * 3}s $data".format(code))
    }
  }

  def loadByteCode(classFile: File): Array[Byte] = {
    // load the binary
    val baos = new ByteArrayOutputStream(0x10000)
    new FileInputStream(classFile) use { fis =>
      IOUtils.copy(fis, baos)
    }

    // decompress & decode the byte code
    decompress(baos.toByteArray)
  }

  def loadClass(classFile: File): OpCode = decompileByteCode(loadByteCode(classFile))

  def saveClass(classFile: File, code: OpCode) = {
    import java.io.FileOutputStream

    new FileOutputStream(classFile) use { fos =>
      fos.write(compress(compileByteCode(code)))
    }
  }

  def pretty(opCode: OpCode, level: Int = 0): List[String] = {
    def mkParams(params: Seq[String]) = params mkString ", "

    val tabs = " " * (level * 4)
    opCode match {
      case ClassNew(name, params, clops) =>
        clops match {
          case CodeBlock(ops) =>
            s"${tabs}class $name(${mkParams(params)}) {" ::
              (ops flatMap (op => pretty(op, level + 1)) toList) :::
              s"$tabs}" :: Nil
          case op =>
            s"${tabs}class $name(${mkParams(params)}) $op" :: Nil
        }

      case CodeBlock(ops) =>
        s"$tabs{" ::
          (ops flatMap (op => pretty(op, level + 1)) toList) :::
          s"$tabs}" :: Nil

      case FunctionNew(name, params, fops) =>
        fops match {
          case CodeBlock(ops) =>
            s"${tabs}def $name(${mkParams(params)}) {" ::
              (ops flatMap (op => pretty(op, level + 1)) toList) :::
              s"$tabs}" :: Nil
          case op =>
            s"${tabs}def $name(${mkParams(params)}) $op" :: Nil
        }

      case op => (tabs + op) :: Nil
    }
  }

  def vmVersion = f"$MAJOR_VERSION.$MINOR_VERSION%02d"

  private def encode(opCode: OpCode)(implicit qs: VSBinary): List[Byte] = {
    opCode match {
      case ArrayIndexRef(entity, index) => ARR_IDX_REF :: encode(entity) ::: encode(index)
      case ArrayValue(elems) => ARR_VAL :: encodeValues(elems)
      case AsyncOp(op) => ASYNC_OP :: encode(op)
      case ClassNew(name, params, code) => CLASS_NEW :: encodeName(name) ::: encodeArray(params) ::: encode(code)
      case CodeBlock(ops) => CODE_BLOCK :: encodeOps(ops)
      case ConstantValue(value) => CONST_VAL :: encodeConstant(value)
      case Decr(entity) => DECR :: encode(entity)
      case Expression(operator, lvalue, rvalue) => EXPRESSION :: encodeSymbol(operator) :: encode(lvalue) ::: encode(rvalue)
      case ForEach(varName, collection, code) => FOR_EACH :: encodeName(varName) ::: encode(collection) ::: encode(code)
      case ForLoop(initial, condition, code) => FOR_LOOP :: encode(initial) ::: encode(condition) ::: encode(code)
      case FunctionCall(entity, args) => FUNC_CALL :: encode(entity) ::: encodeValues(args)
      case FunctionNew(name, params, code) => FUNC_NEW :: encodeName(name) ::: encodeArray(params) ::: encode(code)
      case IfElse(condition, positive, negative) => IF_ELSE :: encode(condition) ::: encode(positive) ::: encode(negative)
      case Import(pathRef) => IMPORT_CODE :: encode(pathRef)
      case Incr(entity) => INCR :: encode(entity)
      case NamedEntityRef(name) => NAMED_ENT_REF :: encodeName(name)
      case NativeOpCode(classRef) => NATIVE_CODE :: encode(classRef)
      case NoOp => NO_OP :: Nil
      case ObjectFieldRef(entity, field) => OBJ_FLD_REF :: encode(entity) ::: encodeName(field)
      case ObjectMethodRef(parent, child) => OBJ_METH_REF :: encode(parent) ::: encode(child)
      case ObjectNew(className, args) => OBJ_NEW :: encodeName(className) ::: encodeValues(args)
      case Println(value) => PRNT_LN :: encode(value)
      case VariableNew(name, value) => VAR_NEW :: encodeName(name) ::: encode(value)
      case WhileDo(condition, code) => WHILE_DO :: encode(condition) ::: encode(code)
      case code =>
        throw new IllegalArgumentException(s"Unrecognized instruction '${code.getClass.getSimpleName}'")
    }
  }

  private def decode(qs: VSBinary): OpCode = {
    qs.cs.get match {
      case ARR_IDX_REF => ArrayIndexRef(decode(qs), decode(qs))
      case ARR_VAL => ArrayValue(decodeValues(qs))
      case ASYNC_OP => AsyncOp(decode(qs))
      case CLASS_NEW => ClassNew(decodeName(qs), decodeArray(qs), decode(qs))
      case CODE_BLOCK => CodeBlock(decodeOps(qs))
      case CONST_VAL => ConstantValue(decodeConstant(qs))
      case DECR => Decr(decode(qs))
      case FOR_EACH => ForEach(decodeName(qs), decode(qs), decode(qs))
      case FOR_LOOP => ForLoop(decode(qs), decode(qs), decode(qs))
      case FUNC_CALL => FunctionCall(decode(qs), decodeValues(qs))
      case FUNC_NEW => FunctionNew(decodeName(qs), decodeArray(qs), decode(qs))
      case IF_ELSE => IfElse(decode(qs), decode(qs), decode(qs))
      case IMPORT_CODE => Import(decode(qs))
      case INCR => Incr(decode(qs))
      case NAMED_ENT_REF => NamedEntityRef(decodeName(qs))
      case NATIVE_CODE => NativeOpCode(decode(qs))
      case NO_OP => NoOp
      case OBJ_FLD_REF => ObjectFieldRef(decode(qs), decodeName(qs))
      case OBJ_METH_REF => ObjectMethodRef(decode(qs), decode(qs))
      case OBJ_NEW => ObjectNew(decodeName(qs), decodeValues(qs))
      case EXPRESSION => Expression(decodeSymbol(qs), decode(qs), decode(qs))
      case PRNT_LN => Println(decode(qs))
      case VAR_NEW => VariableNew(decodeName(qs), decode(qs))
      case WHILE_DO => WhileDo(decode(qs), decode(qs))
      case opCode =>
        throw new IllegalArgumentException(f"Unrecognized opCode '$opCode%02x'")
    }
  }

  private def decodeArray(qs: VSBinary): Seq[String] = qs.nextSeq

  /**
   * Decodes a constant value
   */
  private def decodeConstant(qs: VSBinary) = Some(qs.nextValue)

  /**
   * Decodes an identifier
   */
  private def decodeName(qs: VSBinary): String = qs.nextString

  private def decodeOps(qs: VSBinary): Seq[OpCode] = {
    val count = qs.nextShort
    for (idx <- 1 to count) yield {
      decode(qs)
    }
  }

  private def decodeSymbol(qs: VSBinary): String = {
    val code = qs.next
    BYTE_TO_OPERATORS.get(code) match {
      case Some(symbol) => symbol
      case None =>
        throw new IllegalArgumentException(f"Unrecognized operator code $code%02x")
    }
  }

  private def decodeValues(qs: VSBinary): Seq[OpCode] = {
    val count = qs.nextShort
    for (idx <- 1 to count) yield decode(qs)
  }

  private def encodeArray(values: Seq[String])(implicit qs: VSBinary) = qs.putSeq(values)

  /**
   * Encodes a constant value
   */
  private def encodeConstant(value: Option[Any])(implicit qs: VSBinary) = qs.putValue(value getOrElse "")

  /**
   * Encodes an identifier
   */
  private def encodeName(name: String)(implicit qs: VSBinary) = qs.putString(name)

  private def encodeOps(ops: Seq[OpCode])(implicit qs: VSBinary) = {
    asBytes(ops.length.toShort) ::: ((ops map encode toList) flatten)
  }

  private def encodeSymbol(symbol: String)(implicit qs: VSBinary): Byte = {
    OPERATORS_TO_BYTE.get(symbol) match {
      case Some(code) => code
      case None =>
        throw new IllegalArgumentException(s"Unrecognized symbol or operator `$symbol`")
    }
  }

  private def encodeValues(values: Seq[OpCode])(implicit qs: VSBinary): List[Byte] = {
    asBytes(values.length.toShort) ::: (values flatMap encode toList)
  }

  private def unassemble(qs: VSBinary, level: Int = 0): List[AssyCode] = {
    // capture the current code position
    val p = qs.cs.position()

    // define the helper methods
    def asm(level: Int, statement: String, opCode: String = code(qs, p)) = AssyCode(tabs(level) + statement, opCode)
    def tabs(level: Int) = " " * (level * 2)
    def code(qs: VSBinary, p0: Int, delta: Int = 0): String = {
      val p1 = qs.cs.position()
      val length = (p1 - p0) + delta
      val bytes = new Array[Byte](length)
      qs.cs.position(p0)
      qs.cs.get(bytes)
      qs.cs.position(p1)
      bytes map ("%02x".format(_)) mkString " "
    }

    // interpret the code
    qs.cs.get match {
      case ARR_IDX_REF => asm(level, "arr_idex_ref", code(qs, p + 1)) :: unassemble(qs, level + 1) ::: unassemble(qs, level + 1)
      case ARR_VAL =>
        val count = qs.nextShort
        asm(level, "[") :: unassembleOps(qs, count, level + 1) ::: asm(level, "]", "") :: Nil
      case ASYNC_OP => asm(level, "async") :: unassemble(qs, level + 1)
      case CLASS_NEW => asm(level, s"class ${decodeName(qs)}(${unassembleArray(qs)})") :: unassemble(qs, level + 1)
      case CODE_BLOCK =>
        val count = qs.nextShort
        asm(level, "{") :: unassembleOps(qs, count, level + 1) ::: asm(level, "}", "") :: Nil
      case CONST_VAL => asm(level, unassembleConstant(qs) map (_.toString) getOrElse "") :: Nil
      case DECR => asm(level, "--") :: unassemble(qs, level + 1)
      case FOR_EACH => asm(level, s"foreach ${decodeName(qs)}") :: unassemble(qs, level + 1) ::: unassemble(qs, level + 1)
      case FOR_LOOP => asm(level, "for") :: unassemble(qs, level + 1) ::: unassemble(qs, level + 1) ::: unassemble(qs, level + 1)
      case FUNC_CALL => asm(level, "<invoke>") :: unassemble(qs, level + 1) ::: unassembleOps(qs, qs.nextShort, level + 1)
      case FUNC_NEW => asm(level, s"def ${decodeName(qs)}(${decodeArray(qs) mkString ", "})") :: unassemble(qs, level + 1)
      case IF_ELSE => asm(level, "if") :: unassemble(qs, level + 1)
      case IMPORT_CODE => asm(level, "import") :: unassemble(qs, level + 1)
      case INCR => asm(level, "++") :: unassemble(qs, level + 1)
      case NAMED_ENT_REF => asm(level, decodeName(qs)) :: Nil
      case NATIVE_CODE => asm(level, "native") :: unassemble(qs, level + 1)
      case NO_OP => asm(level, "noop") :: Nil
      case OBJ_FLD_REF => asm(level, s"obj_fld_ref ${decodeName(qs)}") :: unassemble(qs, level + 1)
      case OBJ_METH_REF => asm(level, "::") :: unassemble(qs, level + 1) ::: unassemble(qs, level + 1)
      case OBJ_NEW => asm(level, s"new ${decodeName(qs)}(...)") :: unassembleOps(qs, qs.nextShort, level + 1)
      case EXPRESSION => asm(level, decodeSymbol(qs)) :: unassemble(qs, level + 1) ::: unassemble(qs, level + 1)
      case PRNT_LN => asm(level, "println") :: unassemble(qs, level + 1)
      case QUERY => asm(level, "query") :: unassemble(qs, level + 1)
      case VAR_NEW => asm(level, s"val ${decodeName(qs)} =") :: unassemble(qs, level + 1)
      case WHILE_DO => asm(level, "while") :: unassemble(qs, level + 1) ::: unassemble(qs, level + 1)
      case opCode =>
        throw new IllegalArgumentException(f"Unrecognized opCode '$opCode%02x'")
    }
  }

  /**
   * Decodes a constant value
   */
  private def unassembleConstant(qs: VSBinary) = qs.nextValue match {
    case s: String => Some( s""""$s"""")
    case x => Some(x)
  }

  private def unassembleOps(qs: VSBinary, count: Int, level: Int): List[AssyCode] = {
    ((1 to count) flatMap (n => unassemble(qs, level))).toList
  }

  private def unassembleArray(qs: VSBinary): String = decodeArray(qs) mkString ", "

  def asBytes(v: Byte): List[Byte] = List[Byte](v)

  def asBytes(v: Int): List[Byte] = toByteList(ByteBuffer.allocate(4).putInt(v))

  def asBytes(v: Long): List[Byte] = toByteList(ByteBuffer.allocate(8).putLong(v))

  def asBytes(v: Double): List[Byte] = toByteList(ByteBuffer.allocate(8).putDouble(v))

  def asBytes(v: Short): List[Byte] = toByteList(ByteBuffer.allocate(2).putShort(v))

  def toByteList(buf: ByteBuffer): List[Byte] = {
    val bytes = new Array[Byte](buf.position)
    buf.rewind()
    buf.get(bytes, 0, bytes.length)
    bytes.toList
  }

  def compress(bytes: Array[Byte]): Array[Byte] = {
    import java.io.ByteArrayOutputStream
    import java.util.zip.GZIPOutputStream

    val out = new ByteArrayOutputStream(65535)
    val gzos = new GZIPOutputStream(out)
    gzos.write(bytes)
    gzos.finish()
    gzos.close()
    val cbytes = out.toByteArray
    logger.debug(s"Compressed ${bytes.length} bytes down to ${cbytes.length} bytes")
    cbytes
  }

  def decompress(bytes: Array[Byte]): Array[Byte] = {
    import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
    import java.util.zip.GZIPInputStream

    val buf = new Array[Byte](1024)
    val out = new ByteArrayOutputStream(65535)
    val gzin = new GZIPInputStream(new ByteArrayInputStream(bytes))

    // read the data
    var count = 0
    do {
      count = gzin.read(buf)
      if (count > 0) {
        out.write(buf, 0, count)
      }
    } while (count != -1)
    out.toByteArray
  }

  def extractBytes(bb: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](bb.position())
    bb.rewind()
    bb.get(bytes, 0, bytes.length)
    bytes
  }

  type Offset = Short

  /**
   * Represents an assembly code statement
   */
  case class AssyCode(statement: String, opCode: String = "")

  /**
   * Represents a VScript Binary
   * @author lawrence.daniels@gmail.com
   */
  class VSBinary(val majorVersion: Byte, val minorVersion: Byte, val cs: ByteBuffer, val ds: ByteBuffer) {
    private val constantPool = scala.collection.mutable.Map[Any, Offset]()

    def +=(list: List[Byte]): List[Byte] = {
      cs.put(list toArray)
      list
    }

    def isCompatible = VSBinary.isCompatible(majorVersion, minorVersion)

    def version = f"$majorVersion.$minorVersion%02d"

    def next = cs.get()

    def nextDouble = cs.getDouble

    def nextInt = cs.getInt

    def nextOffset: Offset = nextShort

    def nextSeq = readSeq(nextOffset)

    def nextShort = cs.getShort

    def nextString = readString(nextOffset)

    def nextValue = readValue(nextOffset)

    def put(v: Byte) = cs.put(v)

    def putDouble(v: Int) = cs.putDouble(v)

    def putInt(v: Int) = cs.putInt(v)

    def putShort(v: Int) = cs.putShort(v.toShort)

    def putSeq(seq: Seq[String]) = asBytes(storeSeq(seq))

    def putString(s: String) = asBytes(storeString(s))

    def putValue(v: Any) = asBytes(storeValue(v))

    private def getOffset = ds.position().asInstanceOf[Offset]

    private def readSeq(offset: Offset): Seq[String] = {
      ds.position(offset)
      val length = ds.getShort
      logger.debug(f"Retrieving Seq($length) from offset $offset%04x")
      for (count <- 1 to length) yield {
        val length = ds.getShort
        val bytes = new Array[Byte](length)
        ds.get(bytes)
        new String(bytes)
      }
    }

    private def readString(offset: Offset): String = {
      ds.position(offset)
      val length = ds.getShort.toInt
      val bytes = new Array[Byte](length)
      ds.get(bytes)
      val s = new String(bytes)
      logger.debug(f"Retrieved string ($s) from offset $offset%04x")
      s
    }

    private def readValue(offset: Offset): Any = {
      ds.position(offset)
      val value = ds.get match {
        case TYPE_DOUBLE => ds.getDouble
        case TYPE_INT => ds.getInt
        case TYPE_LONG => ds.getLong
        case TYPE_STRING => readString((offset + 1).asInstanceOf[Offset])
        case vtype => throw new IllegalAccessException(f"Unknown value type - $vtype%02x)")
      }
      logger.debug(f"Retrieved value ($value) from offset $offset%04x")
      value
    }

    private def storeSeq(a: Seq[String]): Offset = {
      val offset = getOffset
      ds.putShort(a.length.toShort)
      a foreach storeString
      offset
    }

    private def storeString(s: String): Offset = {
      constantPool.get(s) match {
        case Some(offset) => offset
        case None =>
          val offset = getOffset
          ds.putShort(s.length.toShort).put(s.getBytes)
          constantPool(s) = offset
          offset
      }
    }

    private def storeValue(value: Any): Offset = {
      constantPool.get(value) match {
        case Some(offset) => offset
        case None =>
          val offset = getOffset
          value match {
            case o: Option[_] => storeValue(o getOrElse "")
            case d: Double => ds.put(TYPE_DOUBLE).putDouble(d)
            case i: Int => ds.put(TYPE_INT).putInt(i)
            case l: Long => ds.put(TYPE_LONG).putLong(l)
            case s: String => ds.put(TYPE_STRING).putShort(s.length.toShort).put(s.getBytes)
            case x => throw new IllegalAccessException(s"Unknown value type - $x (${x.getClass.getSimpleName})")
          }
          constantPool(value) = offset
          offset
      }
    }

    def toArray: Array[Byte] = {
      // get the code and data arrays
      val code = extractBytes(cs)
      val data = extractBytes(ds)
      val header = ((
        asBytes(VS_FILE_ID) ::: MAJOR_VERSION :: MINOR_VERSION ::
          asBytes(code.length.toShort) :::
          asBytes(data.length.toShort)) map (_.toByte)).toArray

      // build the complete binary
      val buf = ByteBuffer.allocate(header.length + code.length + data.length)
      buf.put(header)
      buf.put(code)
      buf.put(data)
      buf.array()
    }
  }

  /**
   * VScript Binary Singleton
   * @author lawrence.daniels@gmail.com
   */
  object VSBinary {

    import java.nio.ByteBuffer.{allocate, wrap}

    def apply() = new VSBinary(MAJOR_VERSION, MINOR_VERSION, allocate(0x8000), allocate(0x8000))

    def isCompatible(majorVersion: Int, minorVersion: Int) = (majorVersion == MAJOR_VERSION) && (minorVersion == MINOR_VERSION)

    def load(bytes: Array[Byte], checkCompatibility: Boolean = true): VSBinary = {
      // wrap the bytes in a buffer
      val buf = ByteBuffer.wrap(bytes)
      logger.debug(s"bytesLen = ${bytes.length}")

      // check the file ID
      if (buf.getInt != VS_FILE_ID)
        throw new IllegalStateException("Invalid or corrupted VScript binary")

      // check the QS version
      val (majorVersion, minorVersion) = (buf.get(), buf.get())
      if (checkCompatibility && isCompatible(majorVersion, minorVersion))
        throw new IllegalStateException(s"Incompatible versions: Binary (v$majorVersion.$minorVersion) - VM (v$MAJOR_VERSION.$MINOR_VERSION)")

      // extract the code and data segments
      val code = new Array[Byte](buf.getShort)
      val data = new Array[Byte](buf.getShort)
      buf.get(code)
      buf.get(data)

      // return the binary
      new VSBinary(majorVersion, minorVersion, wrap(code), wrap(data))
    }
  }

  /////////////////////////////////////////////////////////
  //        OpCode definitions
  /////////////////////////////////////////////////////////

  // assignment & mathematical operations
  private val DECR: Byte = 0x34
  private val INCR: Byte = 0x38

  // flow control operations
  private val FOR_EACH: Byte = 0x22
  private val FOR_LOOP: Byte = 0x25
  private val IF_ELSE: Byte = 0x28
  private val NO_OP: Byte = 0x2a
  private val WHILE_DO: Byte = 0x2f

  // instantiation operations
  private val CLASS_NEW: Byte = 0x14
  private val FUNC_NEW: Byte = 0x18
  private val OBJ_NEW: Byte = 0x1c
  private val VAR_NEW: Byte = 0x1f

  // I/O operations
  private val PRNT_LN: Byte = 0x48

  // operation abstractions & containers
  private val ASYNC_OP: Byte = 0x54
  private val CODE_BLOCK: Byte = 0x58
  private val NATIVE_CODE: Byte = 0x5c
  private val EXPRESSION: Byte = 0x5f
  private val IMPORT_CODE: Byte = 0x5a

  // value references
  private val ARR_IDX_REF: Byte = 0x62
  private val ARR_VAL: Byte = 0x64
  private val CONST_VAL: Byte = 0x66
  private val FUNC_CALL: Byte = 0x68
  private val NAMED_ENT_REF: Byte = 0x6a
  private val OBJ_FLD_REF: Byte = 0x6c
  private val OBJ_METH_REF: Byte = 0x6e
  private val QUERY: Byte = 0x6f

  /////////////////////////////////////////////////////////
  //        Data type definitions
  /////////////////////////////////////////////////////////

  // define the value types
  private val TYPE_STRING: Byte = 1
  private val TYPE_INT: Byte = 2
  private val TYPE_LONG: Byte = 3
  private val TYPE_DOUBLE: Byte = 4

  // define the operator mapping
  val OPERATORS_TO_BYTE = {
    val ops = VScriptCompiler.OPERATORS
    Map(ops zip (1 to ops.size) map { case (k, v) => (k, v.toByte)}: _*)
  }

  val BYTE_TO_OPERATORS = OPERATORS_TO_BYTE map { case (k, v) => (v, k)}

}