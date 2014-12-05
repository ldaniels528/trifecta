/**
 * Queries Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta').controller('QueryCtrl', ['$scope', '$log', '$timeout', 'QuerySvc',
        function ($scope, $log, $timeout, QuerySvc) {

            $scope.running = false;
            var queryStartTime = 0;
            $scope.queryElaspedTime = 0;
            $scope.queryString = "";

            // save queries
            $scope.savedQueries = [];
            $scope.savedQuery = null;

            $scope.results = null;
            $scope.mappings = null;
            $scope.sortField = null;
            $scope.ascending = false;

            /**
             * Downloads the query results
             */
            $scope.downloadResults = function() {
                // TODO future feature
            };

            /**
             * Adds a query to the Saved Queries list
             */
            $scope.queryAdd = function() {
                $scope.savedQueries.push({
                    "name": makeQueryName($scope.queryString),
                    "queryString": $scope.queryString,
                    "count": null,
                    "loading": false
                });
            };

            /**
             * Removes a query query from the list
             * @param index the index of the query to remove
             */
            $scope.queryDelete = function(index) {
                $scope.savedQueries.splice(index, 1);
            };

            /**
             * Selects a query query from the list
             * @param query the query to select
             */
            $scope.selectQuery = function(query) {
                $scope.savedQuery = query;
                $scope.queryString = query.queryString;
            };

            /**
             * Initializes the reference data
             */
            $scope.initReferenceData = function() {
                QuerySvc.getQueries().then(
                    function(queries) {
                        $scope.savedQueries = queries;
                        if(queries) {
                            $scope.queryString = queries[0].queryString;
                        }
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

                // setup the query clock
                queryStartTime = new Date().getTime();
                updatesQueryClock();

                // execute the query
                QuerySvc.executeQuery($scope.queryString).then(
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

            function updatesQueryClock() {
                $scope.queryElaspedTime = (new Date().getTime() - queryStartTime) / 1000;
                if($scope.running) {
                    $timeout(function () {
                        updatesQueryClock();
                    }, 1000);
                }
            }

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