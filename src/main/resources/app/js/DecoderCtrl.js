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
                            $log.error(err);
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
                };

                $scope.saveSchema = function(schema) {
                    DecoderSvc.saveSchema(schema).then(
                        function(response) {
                            $scope.editMode = false;
                        },
                        function(err) {
                            $log.error(err);
                        }
                    );

                    schema.modified = false;
                };

                $scope.selectDecoder = function(decoder) {
                    $scope.decoder = decoder;
                    var schemas = $scope.decoder.schemas;
                    if(schemas.length) {
                        $scope.schema = schemas[0];
                    }
                };

                $scope.selectSchema = function(schema) {
                    $scope.schema = schema;
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