package com.ldaniels528.verify.modules.avro

import com.ldaniels528.verify.VxRuntimeContext
import com.ldaniels528.verify.modules.CommandParser.UnixLikeArgs
import com.ldaniels528.verify.modules.{SimpleParams, Command, Module}
import com.ldaniels528.verify.support.avro.AvroReading
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
    Command(this, "avcat", cat, SimpleParams(required = Seq("variable")), help = "Displays the contents of a schema variable", promptAware = false),
    Command(this, "avload", loadSchema, SimpleParams(required = Seq("variable", "schemaPath")), help = "Loads an Avro schema into memory", promptAware = false)
  )

  override def getVariables: Seq[Variable] = Seq.empty

  def cat(params: UnixLikeArgs): String = {
    val name = params.args.head

    implicit val scope = rt.scope
    val decoder = getAvroDecoder(name)
    decoder.schemaString
  }

  def loadSchema(params: UnixLikeArgs): Unit = {
    val Seq(name, schemaPath, _*) = params.args

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
