/**
 * Decoder Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('DecoderCtrl', ['$scope', '$log', '$parse', '$timeout', 'DecoderSvc',
            function ($scope, $log, $parse, $timeout, DecoderSvc) {

                $scope.decoders = [];
                $scope.decoder = null;
                $scope.schema = null;

                /**
                 * Cancels the edit workflow for a schema; and reverts the schema back to its original content
                 * @param schema the given schema
                 */
                $scope.cancelEdit = function(schema) {
                    if(schema.editMode) {
                        schema.editMode = false;
                        schema.schemaString = schema.originalSchemaString;
                        schema.modified = false;
                    }
                };

                /**
                 * Cancels the new schema workflow
<<<<<<< HEAD
                 * @param decoder the given decoder
                 * @param schema the given schema
                 */
                $scope.cancelNewSchema = function(decoder, schema) {
                    schema.newSchema = false;
                    schema.editMode = false;
                    var index = decoder.schemas.indexOf(schema);
                    if(index != -1) {
                        decoder.splice(index, 1);
                    }
=======
                 * @param schema the given schema
                 */
                $scope.cancelNewSchema = function(schema) {
                    schema.newSchema = false;
                    schema.editMode = false;
                    $scope.reloadDecoders();
>>>>>>> a98e28adb03f56719b52fc5109bf761e0850d6e3
                };

                /**
                 * Reloads the given decoder
                 * @param decoder the given decoder
                 */
                $scope.reloadDecoder = function(decoder) {
                    $log.error("reloadDecoder is not yet implemented");
                    $scope.addErrorMessage("The requested feature is not yet implemented");
                };

                /**
                 * Reloads all decoders; selecting the first one in the list by default
                 */
                $scope.reloadDecoders = function() {
                    DecoderSvc.getDecoders().then(
                        function(decoders) {
                            $scope.decoders = decoders;
                            if(decoders.length) {
                                $scope.selectDecoder(decoders[0]);
                            }
                        },
                        function(err) {
                            $scope.addError(err);
                        });
                };

                /**
                 * Uploads a new schema to the remote server
                 * @param decoder the decoder containing the schema
                 * @param schema the new schema
                 */
                $scope.saveNewSchema = function(decoder, schema) {
                    schema.processing = true;
                    DecoderSvc.saveSchema(schema).then(
                        function(response) {
                            $timeout(function() {
                                schema.processing = false;
                            }, 1000);

                            if(response.error) {
                                $scope.addErrorMessage(response.message);
                            }
                            else {
                                schema.newSchema = false;
<<<<<<< HEAD
=======
                                schema.transitional = false;
>>>>>>> a98e28adb03f56719b52fc5109bf761e0850d6e3
                                schema.editMode = false;
                                schema.modified = false;

                                // attach the schema and select it
                                decoder.schemas.push(schema);
                                $scope.selectDecoder(decoder);
                                $scope.selectSchema(schema);
                            }
                        },
                        function(err) {
                            schema.processing = false;
                            $scope.addError(err);
                        }
                    );
                };

                /**
                 * Saves (uploads) the schema to the remote server
                 * @param schema the given schema
                 */
                $scope.saveSchema = function(schema) {
                    schema.processing = true;
                    DecoderSvc.saveSchema(schema).then(
                        function(response) {
                            $timeout(function() {
                                schema.processing = false;
                            }, 1000);
                            schema.editMode = false;
                            schema.modified = false;
                            if(response.error) {
                                $scope.addErrorMessage(response.message);
                            }
                        },
                        function(err) {
                            schema.processing = false;
                            $scope.addError(err);
                        }
                    );
                };

                /**
<<<<<<< HEAD
=======
                 * Setups the new schema creation workflow
                 * @param decoder the decoder to associate the schema to
                 */
                $scope.setupNewSchema = function(decoder) {
                    var schema = {
                        "topic": decoder.topic,
                        "name": "untitled.avsc",
                        "originalSchemaString": "",
                        "schemaString": "",
                        "modified": true,
                        "transitional": true
                    };

                    decoder.schemas.push(schema);
                    $scope.selectDecoder(decoder);
                    $scope.selectSchema(schema);
                    schema.editMode = true;
                    schema.newSchema = true;
                };

                /**
>>>>>>> a98e28adb03f56719b52fc5109bf761e0850d6e3
                 * Selects the given decoder
                 * @param decoder the given decoder
                 */
                $scope.selectDecoder = function(decoder) {
                    $scope.decoder = decoder;
                    var schemas = $scope.decoder.schemas;
                    if(schemas.length) {
                        $scope.schema = schemas[0];
                    }
                };

                /**
                 * Selects the given schema
                 * @param schema the given schema
                 */
                $scope.selectSchema = function(schema) {
                    $scope.schema = schema;

                    // if there's an error... enable edit mode
                    if(schema.error) {
                        $scope.toggleEditMode();
                    }
                };

                /**
<<<<<<< HEAD
                 * Setups the new schema creation workflow
                 * @param decoder the decoder to associate the schema to
                 */
                $scope.setupNewSchema = function(decoder) {
                    var schema = {
                        "topic": decoder.topic,
                        "name": "untitled.avsc",
                        "originalSchemaString": "",
                        "schemaString": "",
                        "editMode": true,
                        "modified": true,
                        "newSchema": true
                    };

                    decoder.schemas.push(schema);
                    $scope.selectDecoder(decoder);
                    $scope.selectSchema(schema);
                };

                /**
=======
>>>>>>> a98e28adb03f56719b52fc5109bf761e0850d6e3
                 * Toggles edit mode on/off
                 */
                $scope.toggleEditMode = function(schema) {
                    if(schema) {
                        schema.editMode = !schema.editMode;
                        if (schema.editMode) {
                            $scope.schema.originalSchemaString = $scope.schema.schemaString;
                        }
                    }
                };

            }]);

})();