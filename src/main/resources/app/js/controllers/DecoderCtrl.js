/**
 * Decoder Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('DecoderCtrl', ['$scope', '$log', '$timeout', 'DecoderSvc', 'TopicSvc',
            function ($scope, $log, $timeout, DecoderSvc, TopicSvc) {

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

                /**
                 * Downloads the given schema
                 * @param decoder the given decoder
                 * @param schema the given schema
                 */
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
                 * Expands/collapses the given decoder
                 * @param decoder the given decoder
                 * @param callback the optional callback(schemas) function
                 */
                $scope.expandCollapseDecoder = function(decoder, callback) {
                    decoder.decoderExpanded = !decoder.decoderExpanded;
                    if(decoder.decoderExpanded) {
                        decoder.loading = true;
                        DecoderSvc.getDecoderByTopic(decoder.topic).then(
                            function(theDecoder) {
                                // stop the loading sequence after 1 second
                                $timeout(function() {
                                    decoder.loading = false;
                                }, 1000);

                                // store the schemas
                                decoder.schemas = theDecoder.schemas;
                                enrichDecoder(decoder);

                                // perform the callback with the schemas
                                if(callback) callback(decoder.schemas);
                                else {
                                    $scope.selectDecoder(decoder);
                                }
                            },
                            function(err) {
                                decoder.loading = false;
                                $scope.addError(err);
                                if(callback) callback([]);
                            });
                    }
                };

                /**
                 * Returns the icon for the given schema
                 * @param schema the given schema
                 * @returns {string}
                 */
                $scope.getSchemaIcon = function(schema) {
                    if(schema.error) return "/app/images/tabs/decoders/failed-16.png";
                    else if(schema.modified) return "/app/images/tabs/decoders/modified-16.gif";
                    else if(schema.processing) return "/app/images/status/processing.gif";
                    else return "/app/images/tabs/decoders/js-16.png";
                };

                /**
                 * Reloads the given decoder
                 * @param decoder the given decoder
                 */
                $scope.reloadDecoder = function(decoder) {
                    decoder.loading = true;
                    DecoderSvc.getDecoderByTopic(decoder.topic).then(
                        function(loadedDecoder) {
                            // stop the loading sequence after 1 second
                            $timeout(function() {
                                decoder.loading = false;
                            }, 1000);

                            decoder.schemas = loadedDecoder.schemas;
                            enrichDecoder(decoder);
                        },
                        function(err) {
                            decoder.loading = false;
                            $scope.addError(err);
                        });
                };

                /**
                 * Uploads a new schema to the remote server
                 * @param schema the new schema
                 */
                $scope.saveNewSchema = function(schema) {
                    // validate the form
                    if(!validSchemaForSaving(schema)) return;

                    var myDecoder = schema.decoder;
                    schema.processing = true;
                    DecoderSvc.saveDecoderSchema(schema).then(
                        function(response) {
                            $timeout(function() {
                                schema.processing = false;
                            }, 1000);

                            if(response.type == 'error') {
                                $scope.addErrorMessage(response.message);
                            }

                            // refresh the schema list
                            $scope.reloadDecoder(myDecoder);
                        },
                        function(err) {
                            schema.processing = false;
                            $scope.addError(err);
                        }
                    );
                };

                $scope.setUpNewDecoderSchema = function(decoder) {
                    decoder.schemas = decoder.schemas || [];
                    decoder.schemas.push(newDecoderSchema(decoder));
                    $scope.schema = decoder.schemas[decoder.schemas.length - 1];
                };

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

                            // refresh the schema list
                            $scope.reloadDecoder(schema.decoder);
                        },
                        function(err) {
                            schema.processing = false;
                            $scope.addError(err);
                        }
                    );
                };

                /**
                 * Selects the given decoder
                 * @param decoder the given decoder (topic)
                 */
                $scope.selectDecoder = function(decoder) {
                    $scope.decoder = decoder;

                    // ensure the topic is expanded
                    if(!decoder.decoderExpanded) {
                        $scope.expandCollapseDecoder(decoder, function(schemas) {
                            $scope.schema = schemas.length ? schemas[0] : null;
                        });
                    }
                    else {
                        $scope.schema = decoder.schemas.length ? decoder.schemas[0] : null;
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

                $scope.switchToDecoderByTopic = function(decoder) {
                    $scope.selectDecoder(decoder);
                    return true;
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

                function enrichDecoder(decoder) {
                    angular.forEach(decoder.schemas, function(schema) {
                        schema.decoder = decoder;
                    });
                }

                function getUntitledName(decoder) {
                    var index = 0;
                    var name = null;

                    do {
                        index++;
                        name = "Untitled" + index;
                    } while(nameExists(decoder, name));
                    return name;
                }

                function newDecoderSchema(decoder) {
                    return {
                        "decoder": decoder,
                        "topic": decoder.topic,
                        "name": getUntitledName(decoder),
                        "originalSchemaString": "",
                        "schemaString": "",
                        "editMode": true,
                        "modified": true,
                        "newSchema": true
                    };
                }

                /**
                 * Indicates whether the given saved query (name) exists
                 * @param decoder the parent topic
                 * @param name the saved query name
                 * @returns {boolean}
                 */
                function nameExists(decoder, name) {
                    for(var n = 0; n < decoder.schemas.length; n++) {
                        if(name == decoder.schemas[n].name) return true;
                    }
                    return false;
                }

                /**
                 * Validates the schema form for persistence
                 * @param schema the given schema
                 * @returns {boolean}
                 */
                function validSchemaForSaving(schema) {
                    $scope.removeAllMessages();
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
                 * Watch for topic changes, and select the first non-empty topic
                 */
                $scope.$watchCollection("TopicSvc.topics", function(newTopics, oldTopics) {
                    if(!$scope.decoder && newTopics.length) {
                        var decoder = TopicSvc.findNonEmptyTopic();
                        $log.info("Setting first decoder " + decoder.topic);
                        $scope.selectDecoder(decoder);
                    }
                });

            }]);

})();