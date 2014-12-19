/**
 * Decoder Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('DecoderCtrl', ['$scope', '$log', '$timeout', 'DecoderSvc',
            function ($scope, $log, $timeout, DecoderSvc) {

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
                 * @param decoder the given decoder
                 * @param schema the given schema
                 */
                $scope.cancelNewSchema = function(decoder, schema) {
                    if(schema.newSchema) {
                        // remove the schema from the decoder
                        var index = decoder.schemas.indexOf(schema);
                        if (index != -1) {
                            decoder.schemas.splice(index, 1);
                        }

                        // select a different schema
                        var mySchema = decoder.schemas.length ? decoder.schemas[0] : null;
                        if(mySchema) {
                            $scope.selectSchema(mySchema);
                        }
                        else {
                            $scope.schema = null;
                        }
                    }
                };

                $scope.downloadSchema = function(decoder, schema) {
                    var topic = decoder.topic;
                    var schemaName = schema.name;

                    DecoderSvc.downloadDecoderSchema(topic, schemaName).then(
                        function(response) {
                            //$log.info("response = " + angular.toJson(response));
                        },
                        function(err) {
                            $scope.addError(err);
                        });
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

                            // every schema should know its parent decoder
                            angular.forEach($scope.decoders, function(d) {
                               angular.forEach(d.schemas, function(s) {
                                   s.decoder = d;
                               });
                            });
                        },
                        function(err) {
                            $scope.addError(err);
                        });
                };

                /**
                 * Setups the new schema creation workflow
                 * @param decoder the decoder to associate the schema to
                 */
                $scope.setupNewSchema = function(decoder) {
                    //$scope.selectDecoder(decoder);
                    $scope.schema = {
                        "topic": decoder.topic,
                        "name": "untitled.avsc",
                        "originalSchemaString": "",
                        "schemaString": "",
                        "editMode": true,
                        "modified": true,
                        "newSchema": true
                    };
                };

                /**
                 * Uploads a new schema to the remote server
                 * @param decoder the decoder containing the schema
                 * @param schema the new schema
                 */
                $scope.saveNewSchema = function(decoder, schema) {
                    // validate the form
                    if(!validateNewSchemaForSaving(schema)) return;

                    schema.processing = true;
                    DecoderSvc.saveDecoderSchema(schema).then(
                        function(response) {
                            $timeout(function() {
                                schema.processing = false;
                            }, 1000);

                            if(response.error) {
                                $scope.addErrorMessage(response.message);
                            }
                            else {
                                DecoderSvc.getDecoders().then(
                                    function(decoders) {
                                       $scope.decoders = decoders;
                                    },
                                    function(err) {
                                        $scope.addError(err);
                                    });
                            }
                        },
                        function(err) {
                            schema.processing = false;
                            $scope.addError(err);
                        }
                    );
                };

                function validateNewSchemaForSaving(schema) {
                    $scope.clearMessages();
                    var errors = 0;

                    if(!schema.topic) {
                        $scope.addErrorMessage("No topic selected");
                        errors += 1;
                    }
                    if(!schema.name) {
                        $scope.addErrorMessage("No decoder name specified");
                        errors += 1;
                    }
                    if(!schema.schemaString) {
                        $scope.addErrorMessage("No Avro Schema specified");
                        errors += 1;
                    }
                    return errors == 0;
                }

                /**
                 * Saves (uploads) the schema to the remote server
                 * @param schema the given schema
                 */
                $scope.saveSchema = function(schema) {
                    schema.processing = true;
                    DecoderSvc.saveDecoderSchema(schema).then(
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
                 * Selects the given decoder
                 * @param decoder the given decoder
                 */
                $scope.selectDecoder = function(decoder) {
                    $scope.decoder = decoder;
                    decoder.expanded = true;

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
                    $scope.selectDecoder(schema.decoder);
                    $scope.schema = schema;

                    // if there's an error... enable edit mode
                    if(schema.error) {
                        $scope.toggleEditMode();
                    }
                };

                /**
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