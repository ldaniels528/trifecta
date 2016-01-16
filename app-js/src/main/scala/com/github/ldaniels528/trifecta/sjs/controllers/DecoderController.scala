package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.scalascript.core.TimerConversions._
import com.github.ldaniels528.scalascript.core._
import com.github.ldaniels528.scalascript.extensions.Toaster
import com.github.ldaniels528.scalascript.util.ScalaJsHelper._
import com.github.ldaniels528.scalascript.{Controller, Scope, injected}
import com.github.ldaniels528.trifecta.sjs.controllers.ReferenceDataAware._
import com.github.ldaniels528.trifecta.sjs.models._
import com.github.ldaniels528.trifecta.sjs.services.DecoderService
import org.scalajs.dom
import org.scalajs.dom.console

import scala.concurrent.duration._
import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.util.{Failure, Success}

/**
  * Decoder Controller
  *
  * @author lawrence.daniels@gmail.com
  */
class DecoderController($scope: DecoderControllerScope, $log: Log, $timeout: Timeout, toaster: Toaster,
                        @injected("DecoderSvc") decoderSvc: DecoderService)
  extends Controller {

  implicit val scope = $scope

  $scope.decoder = js.undefined
  $scope.decoders = emptyArray
  $scope.schema = js.undefined

  ///////////////////////////////////////////////////////////////////////////
  //    Initialization Functions
  ///////////////////////////////////////////////////////////////////////////

  $scope.init = () => {
    console.log("Initializing Decoder Controller...")
    decoderSvc.getDecoders onComplete {
      case Success(decoders) =>
        $scope.decoders = decoders map enrichDecoder
        $scope.decoder = decoders.headOption.orUndefined
        $scope.schema = $scope.decoder.flatMap(_.schemas).flatMap(_.headOption.orUndefined)
        $scope.decoder.foreach { decoder =>
          enrichDecoder(decoder)
          decoder.decoderExpanded = $scope.schema.isDefined
        }
      case Failure(e) =>
        toaster.error("Failed to read decoders", e.displayMessage)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Decoder Functions
  ///////////////////////////////////////////////////////////////////////////

  /**
    * Downloads the given schema
    * @@param decoder the given decoder
    * @@param schema the given schema
    */
  $scope.downloadSchema = (aDecoder: js.UndefOr[Decoder], aSchema: js.UndefOr[DecoderSchema]) => {
    for {
      decoder <- aDecoder
      topic <- decoder.topic
      schema <- aSchema
      schemaName <- schema.name
    } {
      decoderSvc.downloadDecoderSchema(topic, schemaName) onComplete {
        case Success(response) =>
        //$log.info("response = " + angular.toJson(response))
        case Failure(e) =>
          toaster.error("Schema download failed")
      }
    }
  }

  /**
    * Expands/collapses the given decoder
    * @@param decoder the given decoder
    * @@param callback the optional callback(schemas) function
    */
  $scope.expandCollapseDecoder = (aDecoder: js.UndefOr[Decoder]) => {
    for {
      decoder <- aDecoder
      topic <- decoder.topic
    } {
      decoder.decoderExpanded = !decoder.decoderExpanded.contains(true)
      if (decoder.decoderExpanded.contains(true)) {
        decoder.loading = true
        decoderSvc.getDecoderByTopic(topic) onComplete {
          case Success(theDecoder) =>
            // stop the loading sequence after 1 second
            $timeout(() => decoder.loading = false, 1.second)

            // store the schemas
            decoder.schemas = theDecoder.schemas
            enrichDecoder(decoder)

            // perform the callback with the schemas
            $scope.schema = decoder.schemas.toOption.flatMap(_.headOption).orUndefined

          case Failure(e) =>
            decoder.loading = false
            toaster.error(e.displayMessage)
        }
      }
    }
  }

  $scope.getDecoders = () => {
    if ($scope.hideEmptyTopics) $scope.decoders.filter(_.schemas.exists(_.nonEmpty)) else $scope.decoders
  }

  /**
    * Returns the icon for the given schema
    * @@param schema the given schema
    *
    * @return {string}
    */
  $scope.getSchemaIcon = (aSchema: js.UndefOr[DecoderSchema]) => aSchema map { schema =>
    if (schema.error.exists(_.nonBlank)) "/assets/images/tabs/decoders/failed-16.png"
    else if (schema.processing.contains(true)) "/assets/images/status/processing.gif"
    else "/assets/images/tabs/decoders/js-16.png"
  }

  /**
    * Reloads the given decoder
    * @@param decoder the given decoder
    */
  $scope.reloadDecoder = (aDecoder: js.UndefOr[Decoder]) => {
    for {
      decoder <- aDecoder
      topic <- decoder.topic
    } {
      decoder.loading = true
      decoderSvc.getDecoderByTopic(topic) onComplete {
        case Success(loadedDecoder) =>
          // stop the loading sequence after 1 second
          $timeout(() => decoder.loading = false, 1.second)

          decoder.schemas = loadedDecoder.schemas
          enrichDecoder(decoder)

        case Failure(e) =>
          decoder.loading = false
          $scope.addErrorMessage(e.displayMessage)
      }
    }
  }

  /**
    * Selects the given decoder
    * @@param decoder the given decoder (topic)
    */
  $scope.selectDecoder = (aDecoder: js.UndefOr[Decoder]) => {
    $scope.decoder = aDecoder

    aDecoder foreach { decoder =>
      // ensure the topic is expanded
      if (!decoder.decoderExpanded.contains(true)) {
        $scope.expandCollapseDecoder(decoder)
      }
      else {
        $scope.schema = decoder.schemas.toOption.flatMap(_.headOption).orUndefined
      }
    }
  }

  /**
    * Selects the given schema
    * @@param aSchema the given schema
    */
  $scope.selectSchema = (aSchema: js.UndefOr[DecoderSchema]) => aSchema foreach { schema =>
    $scope.selectDecoder(schema.decoder)
    $scope.schema = schema
  }

  $scope.switchToDecoderByTopic = (aDecoder: js.UndefOr[Decoder]) => aDecoder exists { decoder =>
    $scope.selectDecoder(decoder)
    true
  }

  private def enrichDecoder(decoder: Decoder) = {
    decoder.schemas.foreach(_ foreach (_.decoder = decoder))
    decoder
  }

  ///////////////////////////////////////////////////////////////////////////
  //    Event Handler Functions
  ///////////////////////////////////////////////////////////////////////////

  /**
    * Initialize the controller once the reference data has completed loading
    */
  $scope.$on(REFERENCE_DATA_LOADED, (event: dom.Event, data: ReferenceData) => $scope.init())

}

@js.native
trait DecoderControllerScope extends Scope with GlobalLoading with GlobalErrorHandling with GlobalDataAware with ReferenceDataAware {
  // properties
  var decoder: js.UndefOr[Decoder] = js.native
  var decoders: js.Array[Decoder] = js.native
  var schema: js.UndefOr[DecoderSchema] = js.native

  // functions
  var init: js.Function0[Unit] = js.native
  var downloadSchema: js.Function2[js.UndefOr[Decoder], js.UndefOr[DecoderSchema], Unit] = js.native
  var expandCollapseDecoder: js.Function1[js.UndefOr[Decoder], Unit] = js.native
  var getDecoders: js.Function0[js.Array[Decoder]] = js.native
  var getSchemaIcon: js.Function1[js.UndefOr[DecoderSchema], js.UndefOr[String]] = js.native
  var reloadDecoder: js.Function1[js.UndefOr[Decoder], Unit] = js.native
  var selectDecoder: js.Function1[js.UndefOr[Decoder], Unit] = js.native
  var selectSchema: js.Function1[js.UndefOr[DecoderSchema], Unit] = js.native
  var switchToDecoderByTopic: js.Function1[js.UndefOr[Decoder], Boolean] = js.native

}