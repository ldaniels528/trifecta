package com.ldaniels528.verify.vscript

import com.ldaniels528.verify.vscript.VScriptParser._
import com.ldaniels528.verify.vscript.VScriptRuntime._

import scala.collection.mutable

/**
 * VScript Compiler
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object VScriptCompiler {
  private[vscript] val OPERATORS = Seq("::", ":+", "+:", "+", "-", "*", "/", "%", "(", "[", ".", "==", "===")
  private[this] val logger = org.apache.log4j.Logger.getLogger(getClass)
  private[this] val DECIMAL_p = "\\d+\\.?\\d*"
  private[this] val DECIMAL_r = "^(\\d+\\.?\\d*)$".r
  private[vscript] val SOURCE_EXT = ".qs"

  /**
   * For stand alone execution.
   * Syntax: QuoteScriptCompiler <scriptFile1>[, <scriptFile2> .. <scriptFileN>]
   * @param args the given command line arguments
   */
  def main(args: Array[String]) {
    import java.io.File

    /**
     * Returns the corresponding class file for the given script file
     */
    def getClassFile(scriptFile: File) = {
      val srcPath = scriptFile.getAbsolutePath
      val binPath = (if (srcPath.endsWith(SOURCE_EXT)) srcPath.dropRight(SOURCE_EXT.length) else srcPath) + CLASS_EXT
      new File(binPath)
    }

    // make sure arguments were passed
    if (args.isEmpty) {
      throw new IllegalArgumentException("Syntax: QuoteScriptCompiler <scriptFile1>[, <scriptFile2> .. <scriptFileN>]")
    }

    // compile the sources
    val debug = false
    args foreach { arg =>
      val scriptFile = new File(arg)
      val classFile = getClassFile(scriptFile)
      val sourceCode = (io.Source.fromFile(scriptFile).getLines() mkString "\n").trim
      logger.info(s"Compiling '$scriptFile'...")
      VScriptByteCodeUtil.saveClass(classFile, compile(sourceCode, debug))
    }
  }

  /**
   * Compiles the given source into a collection of expressions
   */
  def compile(sourceCode: String, debug: Boolean = false): OpCode = {
    // parse the source into tokens
    val tokens = parse(sourceCode, debug)

    // compile the code
    val opCodes = mutable.Buffer[OpCode]()
    while (tokens.hasNext) {
      nextExpression(tokens) match {
        case EOL | EOS =>
        case opCode => opCodes += opCode
      }
    }

    // return either a single opCode or a code block
    if (opCodes.isEmpty) NoOp
    else if (opCodes.length == 1) opCodes(0)
    else CodeBlock(opCodes.toList)
  }

  /**
   * Compile the next token (or sequence of tokens) into an expression
   */
  protected def nextExpression(ti: TokenIterator): OpCode = {

    def recurse(accum: OpCode, ti: TokenIterator): OpCode = {
      //logger.info(s"nextExpression: accum => [$accum], ti.peek => ${escapeTokens(ti.peek)}")
      ti.peek match {
        // is it an operator?
        case Some(s) if OPERATORS.contains(s) =>
          //logger.info(s"nextExpression: accum => [$accum], operator => $s")
          ti.next match {
            case "." => ObjectFieldRef(accum, expectName(ti))
            case "(" => FunctionCall(accum, encodeArguments(ti.back))
            case "[" => ArrayIndexRef(accum, expectExpression(ti, "]"))
            case "::" => ObjectMethodRef(accum, nextExpression(ti))
            case operator =>
              val value = nextExpression(ti)
              //logger.info(s"nextExpression: value => [$value] (${value.getClass.getSimpleName})")
              if (value != EOL && value != EOS) recurse(Expression(operator, accum, value), ti) else accum
          }

        // unrecognized, move on ...
        case _ => accum
      }
    }

    // initialize the accumulator with the first value of the sequence
    val elem = nextOpCode(ti)
    //logger.info(s"nextExpression: elem => [$elem]")
    if (elem == EOS || elem == EOL) elem else recurse(elem, ti)
  }

  protected def nextInstruction(ti: TokenIterator): OpCode = {
    // skip carriage returns & line feeds
    skipCRLF(ti)

    // have we reached the end-of-the-stream?
    if (!ti.hasNext) EOS
    else {
      //logger.info(s"code => $value (${value.getClass.getSimpleName})")
      val opCode = nextOpCode(ti)
      if (opCode == EOL) EOS else opCode
    }
  }

  private def nextOpCode(ti: TokenIterator): OpCode = {
    ti.nextOption match {
      case None => throw new IllegalStateException("Unexpected end-of-stream")
      case Some(s) => s match {
        case "\n" | "\r" => EOL
        case "[" => ArrayValue(extractSeq(ti.back, "[", "]", Some(","), nextExpression))
        case "(" => expectExpression(ti, ")")
        case "{" => CodeBlock(extractSeq(ti.back, "{", "}", None, nextExpression))
        case ";" => EOS
        case "async" => AsyncOp(nextExpression(ti))
        case "class" => ClassNew(expectName(ti), encodeParameters(ti), nextExpression(ti))
        case "def" => FunctionNew(expectName(ti), encodeParameters(ti), nextExpression(ti))
        case "for" => ForLoop(nextExpression(ti), nextExpression(ti), nextExpression(ti))
        case "foreach" => ForEach(expectName(ti), nextExpression(ti), nextExpression(ti))
        case "if" => IfElse(nextExpression(ti), nextExpression(ti), if (ti.isNext("else")) nextExpression(ti.forward) else NoOp)
        case "import" => Import(nextExpression(ti))
        case "native" => NativeOpCode(nextExpression(ti))
        case "new" => ObjectNew(expectName(ti), encodeArguments(ti))
        case "print" => Print(nextExpression(ti))
        case "println" => Println(nextExpression(ti))
        case "val" => VariableNew(expectName(ti, "="), nextExpression(ti))
        case "while" => WhileDo(nextExpression(ti), nextExpression(ti))
        case a if isQuoted(s) => ConstantValue(Some(unquote(a)))
        case DECIMAL_r(b) => ConstantValue(Some(b.toDouble))
        case name =>
          if (isValidIdentifier(name)) encoreVariableRef(name, ti)
          else throw new IllegalArgumentException(s"Syntax error: invalid identifier or symbol '$name'")
      }
    }
  }

  private def encoreVariableRef(name: String, ti: TokenIterator): OpCode = {
    val MY_OPERATORS = Set("++", "--") // =
    val entity = NamedEntityRef(name)
    ti.peek match {
      case Some(s) if MY_OPERATORS.contains(s) =>
        ti.forward
        s match {
          case "++" => Incr(entity)
          case "--" => Decr(entity)
          case symbol =>
            throw new IllegalArgumentException(s"Syntax error: invalid operator or symbol '$symbol'")
        }
      case _ => entity
    }
  }

  private def encodeArguments(ti: TokenIterator): Seq[OpCode] = extractSeq(ti, "(", ")", Some(","), nextExpression)

  private def encodeParameters(ti: TokenIterator): Seq[String] = extractSeq(ti, "(", ")", Some(","), _.next)

  private def expectExpression(ti: TokenIterator, expectAfter: String*): OpCode = {
    val expr = nextExpression(ti)
    expectAfter foreach ti.expect
    expr
  }

  private def expectName(ti: TokenIterator, expectAfter: String*): String = {
    val name = ti.next
    if (!isValidIdentifier(name))
      throw new IllegalArgumentException(s"Syntax error: invalid identifier or symbol '$name'")
    expectAfter foreach ti.expect
    name
  }

  private def extractSeq[T](ti: TokenIterator, first: String, last: String, sep: Option[String], tx: TokenIterator => T): Seq[T] = {
    var n = 1
    val elems = mutable.Buffer[T]()
    ti.expect(first)
    while (ti.hasNext && !isNext(ti, last)) {
      val elem = tx(ti)
      if (elem != EOS && elem != EOL) elems += elem
      //logger.info(f"seq[${first}..${last}|$n%02d]: elem = [$elem] (${elem.getClass.getSimpleName}); peek=${escapeTokens(ti.peek)}"); n += 1
      sep foreach { s => if (!isNext(ti, last)) ti.expect(s)}
    }
    ti.expect(last)
    elems
  }

  private def isNext(ti: TokenIterator, s: String): Boolean = {
    // skip white space
    skipCRLF(ti)

    // is the token next?
    ti.isNext(s)
  }

  private[vscript] def isValidIdentifier(name: String): Boolean = {
    val ca = name.toCharArray.toSeq
    ca.head.isLetter || ca.head == '_'
  }

  private def skipCRLF(ti: TokenIterator): Boolean = {
    while (ti.hasNext && (ti.isNext("\r") || ti.isNext("\n"))) ti.forward
    ti.hasNext
  }

  private[vscript] def isQuoted(s: String): Boolean = {
    (s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"))
  }

  /**
   * Returns a new string without quotes
   */
  private[vscript] def unquote(s: String) = if (isQuoted(s)) s.drop(1).dropRight(1) else s

  /**
   * Represents the end of a line ('\n' or '\r')
   */
  case object EOL extends OpCode {
    override def eval(implicit scope: Scope) = None

    override def toString = "\n"
  }

  /**
   * Represents the end of a statement (;)
   */
  case object EOS extends OpCode {
    override def eval(implicit scope: Scope) = None

    override def toString = ";"
  }

}