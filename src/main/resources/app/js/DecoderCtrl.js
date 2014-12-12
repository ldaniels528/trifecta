/**
 * Decoder Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('DecoderCtrl', ['$scope', '$log', '$parse', '$timeout', 'DecoderSvc',
            function ($scope, $log, $parse, $timeout, DecoderSvc) {

                var _lastGoodResult = "";
                $scope.decoders = [];
                $scope.decoder = null;
                $scope.schema = null;
                $scope.editMode = false;

                $scope.init = function() {
                    // load the decoders
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

                $scope.cancelEdit = function(schema) {
                    if($scope.editMode) {
                        $scope.editMode = false;
                        $scope.schema.schemaString = $scope.schema.originalSchemaString;
                        $scope.schema.modified = false;
                    }
                };

                $scope.reloadDecoder = function(decoder) {
                    $log.error("reloadDecoder is not yet implemented");
                    $scope.addErrorMessage("The requested feature is not yet implemented");
                };

                $scope.saveNewSchema = function(schema) {
                    schema.processing = true;
                    DecoderSvc.saveSchema(schema).then(
                        function(response) {
                            $timeout(function() {
                                schema.processing = false;
                            }, 1000);
                            schema.transitional = false;
                            $scope.editMode = false;
                            schema.modified = false;
                        },
                        function(err) {
                            schema.processing = false;
                            $scope.addError(err);
                        }
                    );
                };

                $scope.saveSchema = function(schema) {
                    schema.processing = true;
                    DecoderSvc.saveSchema(schema).then(
                        function(response) {
                            $timeout(function() {
                                schema.processing = false;
                            }, 1000);
                            $scope.editMode = false;
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
                    $scope.editMode = true;
                };

                $scope.selectDecoder = function(decoder) {
                    // turn on edit mode, if it's on...
                    if($scope.editMode) {
                        $scope.toggleEditMode();
                    }

                    $scope.decoder = decoder;
                    var schemas = $scope.decoder.schemas;
                    if(schemas.length) {
                        $scope.schema = schemas[0];
                    }
                };

                $scope.selectSchema = function(schema) {
                    // turn on edit mode, if it's on...
                    if($scope.editMode) {
                        $scope.toggleEditMode();
                    }

                    $scope.schema = schema;

                    // if there's an error... enable edit mode
                    if(schema.error) {
                        $scope.toggleEditMode();
                    }
                };

                $scope.toggleEditMode = function() {
                    $scope.editMode = ! $scope.editMode;
                    if($scope.editMode) {
                        $scope.schema.originalSchemaString = $scope.schema.schemaString;
                    }
                };

                /**
                 * Formats a JSON object as a color-coded JSON expression
                 * @param objStr the JSON object
                 * @param tabWidth the number of tabs to use in formatting
                 * @returns {*}
                 */
                $scope.toPrettyJSON = function (objStr, tabWidth) {
                    try {
                        var obj = $parse(objStr)({});
                    } catch (e) {
                        $log.error(e);
                        return _lastGoodResult;
                    }

                    var result = JSON.stringify(obj, null, Number(tabWidth));
                    _lastGoodResult = result;

                    return result;
                };


            }]);

})();