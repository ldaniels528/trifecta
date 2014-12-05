/**
 * Queries Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta').controller('QueriesCtrl', ['$scope', '$log', '$timeout', 'DashboardSvc',
        function ($scope, $log, $timeout, DashboardSvc) {

            $scope.running = false;
            $scope.queryString = "";

            $scope.jobs = [];
            $scope.favorites = [];
            $scope.favoriteIndex = 0;
            $scope.results = null;
            $scope.mappings = null;
            $scope.sortField = null;
            $scope.ascending = false;

            /**
             * Downloads the query results
             */
            $scope.downloadResults = function() {

            };

            /**
             * Adds a query to the Favorites list
             */
            $scope.favoriteAdd = function() {
                $scope.favorites.push({
                    "name": makeQueryName($scope.queryString),
                    "queryString": $scope.queryString,
                    "count": null,
                    "loading": false
                });
            };

            /**
             * Removes a favorite query from the list
             * @param index the index of the query to remove
             */
            $scope.favoriteDelete = function(index) {
                $scope.favorites.splice(index, 1);
            };

            /**
             * Selects a favorite query from the list
             * @param index the index of the query to select
             */
            $scope.favoriteSelect = function(index) {
                $scope.favoriteIndex = index;
                $scope.queryString = $scope.favorites[index].queryString;
            };

            /**
             * Initializes the reference data
             */
            $scope.initReferenceData = function() {
                DashboardSvc.getLastQuery().then(
                    function(response) {
                        $scope.queryString = response.queryString;
                    },
                    function(err) {
                        setError(err);
                    });
            };

            /**
             * Executes the BDQL query representing by the query string
             */
            $scope.executeQuery = function () {
                $scope.results = null;
                $scope.mappings = null;
                $scope.running = true;

                // execute the query
                DashboardSvc.executeQuery($scope.queryString).then(
                    function (results) {
                        $scope.running = false;
                        //$log.info("results = " + angular.toJson(results));
                        if (results.type == 'error') {
                            $scope.addErrorMessage(results.message);
                        }
                        else if (results.type == 'warning') {
                            $scope.addWarningMessage(results.message);
                        }
                        else if (results.type == 'info') {
                            $scope.addInfoMessage(results.message);
                        }
                        else {
                            $scope.results = results;
                            $scope.mappings = generateDataArray(results.labels, results.values);
                            $log.info("mappings = " + angular.toJson($scope.mappings));
                        }
                    },
                    function (err) {
                        $scope.running = false;
                        setError(err);
                    }
                );
            };

            $scope.filterLabels = function (labels) {
                return labels ? labels.slice(0, labels.length - 2) : null
            };

            $scope.offsetAt = function (index) {
                var row = $scope.results.values[index];
                return row["__offset"];
            };

            $scope.partitionAt = function (index) {
                var row = $scope.results.values[index];
                return row["__partition"];
            };

            $scope.saveQuery = function() {
                $log.info("Uploading query string...");
            };

            $scope.toggleSortField = function (sortField) {
                $scope.ascending = $scope.sortField == sortField ? !$scope.ascending : true;
                $scope.sortField = sortField;

                $scope.results.values = sortData($scope.results.values, sortField, $scope.ascending);
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

            function makeQueryName(queryString) {
                var result = (queryString.length < 15) ? queryString : queryString.substring(0, 15);
                if(queryString.length > 40) {
                    result += "..." + queryString.substring(queryString.length - 15, queryString.length);
                }
                return result;
            }

            function setError(err) {
                $scope.addError(err);
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