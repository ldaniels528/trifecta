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
                'select symbol, exchange, lastTrade, open, prevClose, high, low, volume \n' +
                'from "topic:shocktrade.quotes.avro" with default \n' +
                'where volume >= 1,000,000 \n' +
                'and lastTrade <= 1';

            $scope.jobs = [];
            $scope.favorites = [];
            $scope.results = null;
            $scope.mappings = null;
            $scope.sortField = null;
            $scope.ascending = false;

            /**
             * Executes the BDQL query representing by the query string
             */
            $scope.executeQuery = function () {
                $scope.errorMessage = null;
                $scope.results = null;
                $scope.mappings = null;
                $scope.running = true;

                // execute the query
                DashboardSvc.executeQuery($scope.queryString).then(
                    function (results) {
                        $scope.running = false;
                        $log.info("results = " + angular.toJson(results));
                        if (results.type == 'error') {
                            $scope.errorMessage = results.message;
                        }
                        else {
                            $scope.results = results;
                            $scope.mappings = generateDataArray(results.labels, results.values);
                            $log.info("mappings = " + angular.toJson($scope.mappings));
                        }
                    },
                    function (err) {
                        $scope.running = false;
                        $log.error(err);
                    }
                );
            };

            $scope.filterLabels = function (labels) {
                return labels ? labels.slice(0, labels.length - 2) : null
            };

            $scope.toggleSortField = function (sortField) {
                $scope.ascending = $scope.sortField == sortField ? !$scope.ascending : true;
                $scope.sortField = sortField;

                $scope.results.values = sortData($scope.results.values, sortField, $scope.ascending);
            };

            $scope.offsetAt = function (index) {
                var row = $scope.results.values[index];
                return row["__offset"];
            };

            $scope.partitionAt = function (index) {
                var row = $scope.results.values[index];
                return row["__partition"];
            };

            function generateDataArray(allLabels, results) {
                var rows = [];
                var labels = $scope.filterLabels(allLabels);
                angular.forEach(results, function (result) {
                    var row = {};
                    angular.forEach(labels, function (label) {
                        row[label] = result[label];
                    });
                    rows.push(row);
                });

                return {"labels": labels, "values": rows};
            }

            function sortData(results, sortField, ascending) {
                return results.sort(function (aa, bb) {
                    var a = aa[sortField];
                    var b = bb[sortField];
                    return ascending ? (a > b ? 1 : ((a < b) ? -1 : 0)) : (a > b ? -1 : ((a < b) ? 1 : 0));
                });
            }

        }]);

})();