/**
 * Trifecta Inspect Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('InspectCtrl', ['$scope', '$log', '$timeout', 'DashboardSvc', 'MessageSearchSvc',
            function ($scope, $log, $timeout, DashboardSvc, MessageSearchSvc) {

                $scope.hideEmptyTopics = true;

                /**
                 * Converts the given offset from a string value to an integer
                 * @param partition the partition that the offset value will be updated within
                 * @param offset the given offset string value
                 */
                $scope.convertOffsetToInt = function(partition, offset) {
                    partition.offset = parseInt(offset);
                };

            }]);
})();