/**
 * Queries Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta').controller('QueriesCtrl', ['$scope', '$log', '$timeout', 'DashboardSvc',
        function ($scope, $log, $timeout, DashboardSvc) {

            $scope.errorMessage = null;
            $scope.running = false;
            $scope.queryString =
                'select symbol, lastTrade, open, prevClose, high, low, volume \n' +
                'from "shocktrade.quotes.avro" with "avro:file:avro/quotes" \n' +
                'where volume >= 1,000,000 and lastTrade <= 1';

            $scope.jobs = [];
            $scope.favorites = [];

            /**
             * Executes the BDQL query representing by the query string
             */
            $scope.executeQuery = function () {
                $scope.running = true;

                // execute the query
                DashboardSvc.executeQuery($scope.queryString).then(
                    function(results) {
                        $scope.running = false;
                        $log.info("results = " + angular.toJson(results));
                        if(results.type == 'error') {
                            $scope.errorMessage = results.message;
                        }
                    },
                    function(err) {
                        $scope.running = false;
                        $log.error(err);
                    }
                );
            };

        }]);

})();