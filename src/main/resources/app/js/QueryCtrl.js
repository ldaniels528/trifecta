/**
 * Queries Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta').controller('QueryCtrl', ['$scope', '$log', '$timeout', 'QuerySvc',
        function ($scope, $log, $timeout, QuerySvc) {

            var queryStartTime = 0;
            $scope.queryElaspedTime = 0;

            // saved queries
            $scope.savedQueries = [];
            $scope.savedQueries.push(newQueryScript());
            $scope.savedQuery = $scope.savedQueries[0];

            // create the query state object
            $scope.state = createNewQueryState();

            function createNewQueryState() {
                return {
                    "running": false,
                    "results": null,
                    "mappings": null,
                    "ascending": false,
                    "sortField": null
                };
            }

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

                        $scope.savedQueries.push(newQueryScript());
                        $scope.savedQuery = $scope.savedQueries[0];
                    });
            };

            /**
             * Downloads the query results as CSV
             */
            $scope.downloadResults = function(results) {
                QuerySvc.transformResultsToCSV(results).then(
                    function(response) {
                        $log.info("response = " + angular.toJson(response));
                    },
                    function(err) {
                        setError(err)
                    }
                );
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
                $scope.state.results = null;
                $scope.state.mappings = null;
                $scope.state.running = true;

                // setup the query clock
                queryStartTime = new Date().getTime();
                updatesQueryClock();

                // ensure we have an array for storing the results
                if(!$scope.savedQuery.results) {
                    $scope.savedQuery.results = [];
                }

                // keep a reference to the original saved query
                var mySavedQuery =  $scope.savedQuery;
                mySavedQuery.loading = true;

                // execute the query
                QuerySvc.executeQuery(mySavedQuery.queryString).then(
                    function (results) {
                        $scope.state.running = false;
                        mySavedQuery.loading = false;

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
                            $scope.state.results = results;
                            $scope.state.mappings = generateDataArray(results.labels, results.values);
                            //$log.info("mappings = " + angular.toJson($scope.state.mappings));

                            mySavedQuery.results.push({
                                "name": new Date().toTimeString(),
                                "results": results,
                                "mappings": $scope.state.mappings
                            });
                            mySavedQuery.resultIndex = mySavedQuery.results.length - 1;
                        }
                    },
                    function (err) {
                        $scope.state.running = false;
                        mySavedQuery.loading = false;
                        setError(err);
                    }
                );
            };

            /**
             * Filters out topics without messages; returning only the topics containing messages
             * @param topics the given array of topic summaries
             * @returns Array of topics containing messages
             */
            $scope.filterEmptyTopics = function (topics) {
                var filteredTopics = [];
                for (var n = 0; n < topics.length; n++) {
                    var ts = topics[n];
                    if (ts.totalMessages > 0) {
                        filteredTopics.push(ts);
                    }
                }
                return filteredTopics;
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
                var row = $scope.state.results.values[index];
                return row["__offset"];
            };

            $scope.partitionAt = function (index) {
                var row = $scope.state.results.values[index];
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

            $scope.selectQueryResults = function(index) {
                $scope.savedQuery.resultIndex = index;
                var savedResults = $scope.savedQuery.results[index];
                $scope.state.results = savedResults.results;
                $scope.state.mappings = savedResults.mappings;
            };

            $scope.toggleSortField = function (sortField) {
                $scope.state.ascending = $scope.state.sortField == sortField ? !$scope.state.ascending : true;
                $scope.state.sortField = sortField;
                $scope.state.results.values = sortData($scope.state.results.values, sortField, $scope.state.ascending);
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
                if($scope.state.running) {
                    $timeout(function () {
                        updatesQueryClock();
                    }, 1000);
                }
            }

        }]);

})();