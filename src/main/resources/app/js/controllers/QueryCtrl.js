/**
 * Query Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta').controller('QueryCtrl', ['$scope', '$log', '$timeout', 'QuerySvc',
        function ($scope, $log, $timeout, QuerySvc) {

            var queryStartTime = 0;
            $scope.queryElaspedTime = 0;
            $scope.savedQuery = null;
            $scope.hideEmptyTopics = false;

            // create the query state object
            $scope.state = createQuerySession();

            /**
             * Creates a new query session object
             * @returns {{running: boolean, results: null, mappings: null, ascending: boolean, sortField: null}}
             */
            function createQuerySession() {
                return {
                    "running": false,
                    "results": null,
                    "mappings": null,
                    "ascending": false,
                    "sortField": null
                };
            }

            $scope.cancelNewQuery = function(savedQuery) {
                if(savedQuery.newFile) {
                    var topic = savedQuery.topic;

                    // remove the saved query from the list
                    var savedQueries = topic.savedQueries || [];
                    var index = savedQueries.indexOf(savedQuery);
                    if(index != -1) {
                        savedQueries.splice(index, 1);
                    }

                    if($scope.savedQuery == savedQuery) {
                        $scope.savedQuery = null;
                    }
                }
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
             * Downloads the query results as CSV
             */
            $scope.downloadResults = function(results) {
                QuerySvc.transformResultsToCSV(results).then(
                    function(response) {
                        $log.info("response = " + angular.toJson(response));
                    },
                    function(err) {
                        $scope.addError(err);
                    }
                );
            };

            /**
             * Executes the KQL query representing by the query string
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
                QuerySvc.executeQuery(mySavedQuery.name, mySavedQuery.topic, mySavedQuery.queryString).then(
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
                        $scope.addError(err);
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

            $scope.expandTopicQueries = function(topic) {
                topic.queriesExpanded = !topic.queriesExpanded;
                if(topic.queriesExpanded) {
                    loadQueriesByTopic(topic);
                }
            };

            function loadQueriesByTopic(topic) {
                topic.loading = true;
                QuerySvc.getQueriesByTopic(topic.topic).then(
                    function (queries) {
                        topic.loading = false;
                        topic.savedQueries = queries || [];
                    },
                    function (err) {
                        topic.loading = false;
                        $scope.addError(err);
                    });
            }

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
                    $log.info("Uploading query '" + query.name + "' (topic " + query.topic + ")...");
                    QuerySvc.saveQuery(query.name, query.topic, query.queryString).then(
                        function (response) {
                            if(response && response.type == 'error') {
                                $scope.addErrorMessage(response.message);
                                query.syncing = false;
                            }
                            else {
                                query.modified = false;
                                query.newFile = false;
                                query.syncing = false;
                            }
                        },
                        function (err) {
                            query.syncing = false;
                            $scope.addError(err);
                        }
                    );
                }
            };

            $scope.selectQueryResults = function(savedQuery, index) {
                savedQuery.resultIndex = index;
                var savedResults = savedQuery.results[index];
                $scope.state.results = savedResults.results;
                $scope.state.mappings = savedResults.mappings;
            };

            $scope.setUpNewQueryDocument = function(topic) {
                topic.savedQueries = topic.savedQueries || [];
                topic.savedQueries.push(newQueryScript(topic));
                $scope.savedQuery = topic.savedQueries[topic.savedQueries.length - 1];
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

            function getUntitledName(topic) {
                var index = 0;
                var name = null;

                do {
                    index++;
                    name = "Untitled" + index;
                } while(nameExists(topic, name));
                return name;
            }

            /**
             * Indicates whether the given saved query (name) exists
             * @param topic the parent topic
             * @param name the saved query name
             * @returns {boolean}
             */
            function nameExists(topic, name) {
                for(var n = 0; n < topic.savedQueries.length; n++) {
                    if(name == topic.savedQueries[n].name) return true;
                }
                return false;
            }

            /**
             * Constructs a new query script data object
             * @returns {{name: string, topic: string, queryString: string, newFile: boolean, modified: boolean}}
             */
            function newQueryScript(topic) {
                return {
                    "name": getUntitledName(topic),
                    "topic" : topic,
                    "queryString": "",
                    "newFile": true,
                    "modified": true
                };
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