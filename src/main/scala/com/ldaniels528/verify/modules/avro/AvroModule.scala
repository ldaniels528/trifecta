package com.ldaniels528.verify.modules.avro

import com.ldaniels528.verify.VxRuntimeContext
import com.ldaniels528.verify.modules.{Command, Module}
import com.ldaniels528.verify.vscript.{OpCode, Scope, Variable}

/**
 * Avro Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class AvroModule(rt: VxRuntimeContext) extends Module with AvroReading {
  private implicit val rtc = rt

  /**
   * Returns the name of the module (e.g. "avro")
   * @return the name of the module
   */
  override def moduleName: String = "avro"

  /**
   * Returns the commands that are bound to the module
   * @return the commands that are bound to the module
   */
  override def getCommands: Seq[Command] = Seq(
    Command(this, "avcat", cat, (Seq("variable"), Seq.empty), help = "Displays the contents of a schema variable"),
    Command(this, "avload", loadSchema, (Seq("variable", "schemaPath"), Seq.empty), help = "Loads an Avro schema into memory")
  )

  override def getVariables: Seq[Variable] = Seq.empty

  def cat(args: String*): String = {
    val name = args.head

    implicit val scope = rt.scope
    val decoder = getAvroDecoder(name)
    decoder.schemaString
  }

  def loadSchema(args: String*): Unit = {
    val Seq(name, schemaPath, _*) = args

    // get the decoder
    val decoder = loadAvroDecoder(schemaPath)

    // create the variable and attach it to the scope
    rt.scope += Variable(name, new OpCode {
      val value = Some(decoder)

      override def eval(implicit scope: Scope): Option[Any] = value

      override def toString = s"[$name]"
    })
    ()
  }

  /**
   * Called when the application is shutting down
   */
  override def shutdown(): Unit = ()

}
