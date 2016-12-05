package com.github.ldaniels528.trifecta.modules

import com.github.ldaniels528.trifecta.command.UnixLikeArgs
import com.github.ldaniels528.trifecta.messages.{MessageInputSource, MessageOutputSource}

/**
  * Module Command
  * @author lawrence.daniels@gmail.comAgent
  */
trait ModuleCommandAgent {

  protected def die[S](message: String): S = throw new IllegalArgumentException(message)

  protected def dieConfigKey[S](key: String): S = die(s"Configuration key '$key' not defined")

  protected def dieInvalidOutputURL[S](url: String, example: String): S = die(s"Invalid output URL '$url' - Example usage: $example")

  protected def dieNoInputHandler[S](device: MessageInputSource): S = die(s"Unhandled input source $device")

  protected def dieNoOutputHandler[S](device: MessageOutputSource): S = die(s"Unhandled output source $device")

  protected def dieSyntax[S](unixArgs: UnixLikeArgs): S = {
    die( s"""Invalid arguments - use "syntax ${unixArgs.commandName.get}" to see usage""")
  }

}
