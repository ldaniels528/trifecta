/**
 * Query Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta').controller('QueryCtrl', ['$scope', '$log', '$timeout', 'QuerySvc',
        function ($scope, $log, $timeout, QuerySvc) {

            /**
             * Creates a new default saved query instance
             * @returns {{running: boolean, results: null, mappings: null, ascending: boolean, sortField: null}}
             */
            $scope.savedQuery = {
                "name": "UntitledName",
                "topic": "default",
                "newFile": true,
                "modified": true
            };

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
            $scope.executeQuery = function (mySavedQuery) {
                mySavedQuery.running = true;

                // setup the query clock
                mySavedQuery.queryElaspedTime = 0;
                mySavedQuery.queryStartTime = new Date().getTime();
                updatesQueryClock(mySavedQuery);

                // execute the query
                QuerySvc.executeQuery(mySavedQuery.name, mySavedQuery.topic, mySavedQuery.queryString).then(
                    function (results) {
                        mySavedQuery.running = false;

                        if (results.type == 'error') $scope.addErrorMessage(results.message);
                        else if (results.type == 'warning') $scope.addWarningMessage(results.message);
                        else if (results.type == 'info') $scope.addInfoMessage(results.message);
                        else {
                            var mappings = generateDataArray(results.labels, results.values);
                            var mySavedResult = {
                                "name": new Date().toTimeString(),
                                "resultSet": results,
                                "mappings": mappings,
                                "ascending": false,
                                "sortField": null
                            };

                            // make sure the results array exists
                            if(!mySavedQuery.results) {
                                mySavedQuery.results = [];
                            }

                            mySavedQuery.results.push(mySavedResult);
                            mySavedQuery.savedResult = mySavedResult;

                            //$log.info("mySavedResult = " + JSON.stringify(mySavedResult, null, '\t'));
                        }
                    },
                    function (err) {
                        mySavedQuery.running = false;
                        $scope.addError(err);
                    }
                );
            };

            $scope.expandTopicQueries = function(topic) {
                topic.queriesExpanded = !topic.queriesExpanded;
                if(topic.queriesExpanded) {
                    loadQueriesByTopic(topic);
                }
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
                var row = $scope.savedQuery.savedResult.resultSet.values[index];
                return row["__offset"];
            };

            $scope.partitionAt = function (index) {
                var row = $scope.savedQuery.savedResult.resultSet.values[index];
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

            /**
             * Selects a query query from the list
             * @param mySavedQuery the saved query object to select
             */
            $scope.selectQuery = function(mySavedQuery) {
                $scope.savedQuery = mySavedQuery;
            };

            $scope.selectQueryResults = function(mySavedQuery, index) {
                $scope.savedQuery = mySavedQuery;
                mySavedQuery.savedResult = mySavedQuery.results[index];
            };

            $scope.setUpNewQueryDocument = function(topic) {
                topic.savedQueries = topic.savedQueries || [];
                topic.savedQueries.push(newQueryScript(topic));
                $scope.savedQuery = topic.savedQueries[topic.savedQueries.length - 1];
            };

            $scope.toggleSortField = function (mySavedQuery, sortField) {
                var mySavedResult = mySavedQuery.savedResult;
                if(mySavedResult) {
                    mySavedResult.ascending = mySavedResult.sortField == sortField ? !mySavedResult.ascending : true;
                    mySavedResult.sortField = sortField;
                    mySavedResult.resultSet.values = sortData(mySavedResult.resultSet.values, sortField, mySavedResult.ascending);
                }
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
            function updatesQueryClock(mySavedQuery) {
                mySavedQuery.queryElaspedTime = (new Date().getTime() - mySavedQuery.queryStartTime) / 1000;
                if(mySavedQuery.running) {
                    $timeout(function () {
                        updatesQueryClock(mySavedQuery);
                    }, 1000);
                }
            }

        }]);

})();