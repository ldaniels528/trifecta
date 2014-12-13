/**
 * Trifecta Inspect Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('InspectCtrl', ['$scope', '$log', '$parse', 'InspectSvc',
            function ($scope, $log,  $parse, InspectSvc) {

                var _lastGoodResult = "";
                $scope.hideEmptyTopics = true;

                /**
                 * Converts the given offset from a string value to an integer
                 * @param partition the partition that the offset value will be updated within
                 * @param offset the given offset string value
                 */
                $scope.convertOffsetToInt = function(partition, offset) {
                    partition.offset = parseInt(offset);
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