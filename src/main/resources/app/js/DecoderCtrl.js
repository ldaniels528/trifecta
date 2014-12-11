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