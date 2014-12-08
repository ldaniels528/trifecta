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

            // saved queries
            $scope.savedQueries = [];
            $scope.savedQueries.push(newQueryScript());
            $scope.savedQuery = $scope.savedQueries[0];

            $scope.results = null;
            $scope.mappings = null;
            $scope.sortField = null;
            $scope.ascending = false;

            /**
             * Initializes the reference data
             */
            $scope.initReferenceData = function() {
                QuerySvc.getQueries().then(
                    function(queries) {
                        $scope.savedQueries = queries || [];
                        if(!$scope.savedQueries.length) {
                            $scope.savedQueries.push(newQueryScript());
                        }

                        // select the first query
                        $scope.savedQuery = $scope.savedQueries[0];
                    },
                    function(err) {
                        setError(err);
                    });
            };

            /**
             * Downloads the query results
             */
            $scope.downloadResults = function() {
                // TODO future feature
            };

            /**
             * Adds a query to the Saved Queries list
             */
            $scope.addQuery = function() {
                var queryScript = newQueryScript();
                $scope.savedQueries.push(queryScript);
                $scope.savedQuery = queryScript;
            };

            /**
             * Removes a query query from the list
             * @param index the index of the query to remove
             */
            $scope.deleteQuery = function(index) {
                $scope.savedQueries.splice(index, 1);
            };

            /**
             * Selects a query query from the list
             * @param queryScript the query script to select
             */
            $scope.selectQuery = function(queryScript) {
                $scope.savedQuery = queryScript;
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

                // ensure we have an array for storing the results
                if(!$scope.savedQuery.results) {
                    $scope.savedQuery.results = [];
                }

                // keep a reference to the original saved query
                var mySavedQuery =  $scope.savedQuery;

                // execute the query
                QuerySvc.executeQuery($scope.savedQuery.queryString).then(
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
                            //$log.info("mappings = " + angular.toJson($scope.mappings));

                            mySavedQuery.results.push({
                                "name": new Date().toTimeString(),
                                "results": results,
                                "mappings": $scope.mappings
                            });
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

            $scope.isSelected = function (topic, partition, results, index) {
                if(!topic || !partition || !results) return false;
                return topic.topic == results.topic &&
                    partition.partition == $scope.partitionAt(index) &&
                    partition.offset == $scope.offsetAt(index);
            };

            $scope.offsetAt = function (index) {
                var row = $scope.results.values[index];
                return row["__offset"];
            };

            $scope.partitionAt = function (index) {
                var row = $scope.results.values[index];
                return row["__partition"];
            };

            $scope.saveQuery = function (query) {
                if (query) {
                    query.syncing = true;
                    $log.info("Uploading query '" + query.name + "'...");
                    QuerySvc.saveQuery(query.name, query.queryString).then(
                        function (response) {
                            if(response && response.type == 'error') {
                                $scope.addErrorMessage(response.message);
                                query.syncing = false;
                            }
                            else {
                                query.modified = false;
                                query.syncing = false;
                            }
                        },
                        function (err) {
                            query.syncing = false;
                            setError(err);
                        }
                    );
                }
            };

            $scope.showResults = function(savedResults) {
                $scope.results = savedResults.results;
                $scope.mappings = savedResults.mappings;
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

            function getUntitledName() {
                var index = 0;
                var name = null;

                do {
                    index++;
                    name = "Untitled" + index;
                } while(nameExists(name));
                return name;
            }

            /**
             * Indicates whether the given saved query (name) exists
             * @param name the saved query name
             * @returns {boolean}
             */
            function nameExists(name) {
                for(var n = 0; n < $scope.savedQueries.length; n++) {
                    if(name == $scope.savedQueries[n].name) return true;
                }
                return false;
            }

            /**
             * Constructs a new query script data object
             * @returns {{name: string, queryString: string, exists: boolean, modified: boolean}}
             */
            function newQueryScript() {
                return {
                    "name": getUntitledName(),
                    "queryString": "",
                    "exists": false,
                    "modified": true
                };
            }

            /**
             * Error response handler
             * @param err the given error response
             */
            function setError(err) {
                $scope.addError(err);
            }

            /**
             * Sorts the given results by the specified sort field
             * @param results the given array of results
             * @param sortField the given sort field
             * @param ascending the sort direction (ascending or descending)
             * @returns {Array.<T>|*}
             */
            function sortData(results, sortField, ascending) {
                return results.sort(function (aa, bb) {
                    var a = aa[sortField];
                    var b = bb[sortField];
                    return ascending ? (a > b ? 1 : ((a < b) ? -1 : 0)) : (a > b ? -1 : ((a < b) ? 1 : 0));
                });
            }

            /**
             * Schedules the query clock to result until the query has completed.
             */
            function updatesQueryClock() {
                $scope.queryElaspedTime = (new Date().getTime() - queryStartTime) / 1000;
                if($scope.running) {
                    $timeout(function () {
                        updatesQueryClock();
                    }, 1000);
                }
            }

        }]);

})();