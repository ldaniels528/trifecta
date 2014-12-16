/**
 * Trifecta Inspect Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('InspectCtrl', ['$scope', '$interval', '$log', '$parse', '$timeout', 'MessageSvc', 'MessageSearchSvc', 'TopicSvc',
            function ($scope, $interval, $log, $parse, $timeout, MessageSvc, MessageSearchSvc, TopicSvc) {

                var _lastGoodResult = "";

                $scope.version = "0.18.3";
                $scope.hideEmptyTopics = true;
                $scope.replicas = [];
                $scope.topics = [];
                $scope.topic = null;
                $scope.loading = 0;

                $scope.displayMode = {
                    "state" : "message"
                };

                $scope.tabs = [
                    {
                        "name": "Observe",
                        "imageURL": "/app/images/tabs/main/observe.png",
                        "active": false
                    }, {
                        "name": "Inspect",
                        "imageURL": "/app/images/tabs/main/inspect.png",
                        "active": false
                    }, {
                        "name": "Query",
                        "imageURL": "/app/images/tabs/main/query.png",
                        "active": false
                    }, {
                        "name": "Decoders",
                        "imageURL": "/app/images/tabs/main/decoders.png",
                        "active": false
                    }
                ];

                // select the default tab and make it active
                $scope.tab = $scope.tabs[0];
                $scope.tab.active = true;

                /**
                 * Changes the active tab
                 * @param index the given tab index
                 * @param event the given click event
                 */
                $scope.changeTab = function (index, event) {
                    // deactivate the current tab
                    $scope.tab.active = false;

                    // activate the new tab
                    $scope.tab = $scope.tabs[index];
                    $scope.tab.active = true;

                    if (event) {
                        event.preventDefault();
                    }
                };

                $scope.clearMessage = function() {
                    $scope.message = {};
                };

                /**
                 * Converts the given offset from a string value to an integer
                 * @param partition the partition that the offset value will be updated within
                 * @param offset the given offset string value
                 */
                $scope.convertOffsetToInt = function(partition, offset) {
                    partition.offset = parseInt(offset);
                };

                /**
                 * Exports the given message to an external system
                 * @param topic the given topic
                 * @param partition the given partition
                 * @param offset the given offset
                 */
                $scope.exportMessage = function(topic, partition, offset) {
                    alert("Not yet implemented");
                };

                $scope.getMessageData = function (topic, partition, offset) {
                    $scope.clearMessage();
                    $scope.loading++;

                    MessageSvc.getMessage(topic, partition, offset).then(
                        function (message) {
                            $scope.message = message;
                            if($scope.loading) $scope.loading--;
                        },
                        function (err) {
                            $scope.addError(err);
                            if($scope.loading) $scope.loading--;
                        });
                };

                $scope.getMessageKey = function (topic, partition, offset) {
                    $scope.clearMessage();
                    $scope.loading++;

                    MessageSvc.getMessageKey(topic, partition, offset).then(
                        function (message) {
                            $scope.message = message;
                            if($scope.loading) $scope.loading--;
                        },
                        function (err) {
                            $scope.addError(err);
                            if($scope.loading) $scope.loading--;
                        });
                };

                $scope.messageFinderPopup = function () {
                    MessageSearchSvc.finderDialog($scope).then(function (form) {
                        // perform the validation of the form
                        if(!form || !form.topic) {
                            $scope.addErrorMessage("No topic selected")
                        }
                        else if(!form.criteria) {
                            $scope.addErrorMessage("No criteria specified")
                        }
                        else {
                            form.topic = form.topic.topic;
                            if (form.topic && form.criteria) {
                                // display the loading dialog
                                var loadingDialog = MessageSearchSvc.loadingDialog($scope);

                                // perform the search
                                MessageSvc.findOne(form.topic, form.criteria)
                                    .then(function (message) {
                                        loadingDialog.close({});
                                        if (message.type == "error") {
                                            $scope.addErrorMessage(message.message);
                                        }
                                        else {
                                            $scope.message = message;

                                            // find the topic and partition
                                            var myTopic = findTopicByName(message.topic);
                                            if (myTopic) {
                                                var myPartition = findPartitionByID(myTopic, message.partition);
                                                if (myPartition) {
                                                    $scope.topic = myTopic;
                                                    $scope.partition = myPartition;
                                                    $scope.partition.offset = message.offset
                                                }
                                            }
                                        }
                                    });
                            }
                        }
                    });
                };

                $scope.getReplicas = function (topic) {
                    TopicSvc.getReplicas(topic).then(
                        function (replicas) {
                            $scope.replicas = replicas;
                        },
                        function (err) {
                            $scope.addError(err);
                        });
                };

                $scope.getTopics = function (hideEmptyTopics) {
                    return $scope.topics.filter(function (topic) {
                        return !hideEmptyTopics || topic.totalMessages > 0;
                    });
                };

                $scope.loadMessage = function () {
                    var topic = $scope.topic.topic;
                    var partition = $scope.partition.partition;
                    var offset = $scope.partition.offset;

                    switch($scope.displayMode.state) {
                        case "key":
                            $scope.getMessageKey(topic, partition, offset);
                            break;
                        case "message":
                            $scope.getMessageData(topic, partition, offset);
                            break;
                        default:
                            $log.error("Unrecognized display mode (mode = " + mode + ")");
                    }
                };

                $scope.firstMessage = function () {
                    ensureOffset($scope.partition);
                    if ($scope.partition.offset != $scope.partition.startOffset) {
                        $scope.partition.offset = $scope.partition.startOffset;
                        $scope.loadMessage();
                    }
                };

                $scope.lastMessage = function () {
                    ensureOffset($scope.partition);
                    if ($scope.partition.offset != $scope.partition.endOffset) {
                        $scope.partition.offset = $scope.partition.endOffset;
                        $scope.loadMessage();
                    }
                };

                $scope.medianMessage = function() {
                    var partition = $scope.partition;
                    ensureOffset(partition);
                    var median = Math.round(partition.startOffset + (partition.endOffset - partition.startOffset)/2);
                    if (partition.offset != median) {
                        partition.offset = median;
                        $scope.loadMessage();
                    }
                };

                $scope.nextMessage = function () {
                    ensureOffset($scope.partition);
                    var offset = $scope.partition.offset;
                    if (offset < 1 + $scope.partition.endOffset) {
                        $scope.partition.offset += 1;
                        $scope.loadMessage();
                    }
                };

                $scope.previousMessage = function () {
                    ensureOffset($scope.partition);
                    var offset = $scope.partition.offset;
                    if (offset && offset > $scope.partition.startOffset) {
                        $scope.partition.offset -= 1;
                        $scope.loadMessage();
                    }
                };

                $scope.resetMessageState = function(mode, topic, partition, offset) {
                    $log.info("mode = " + mode + ", topic = " + topic + ", partition = " + partition + ", offset = " + offset);
                    switch(mode) {
                        case "key":
                            $scope.getMessageKey(topic, partition, offset);
                            break;
                        case "message":
                            $scope.getMessageData(topic, partition, offset);
                            break;
                        default:
                            $log.error("Unrecognized display mode (mode = " + mode + ")");
                    }
                };

                $scope.switchToMessage = function (topicID, partitionID, offset) {
                    $log.info("switchToMessage: topicID = " + topicID + ", partitionID = " + partitionID + ", offset = " + offset);
                    var topic = findTopicByName(topicID);
                    var partition = topic ? findPartitionByID(topic, partitionID) : null;
                    if (partition) {
                        $scope.topic = topic;
                        $scope.partition = partition;
                        $scope.partition.offset = offset;
                        $scope.loadMessage();
                        $scope.changeTab(1, null); // Query
                    }
                };

                /**
                 * Formats a JSON object as a color-coded JSON expression
                 * @param objStr the JSON object
                 * @param tabWidth the number of tabs to use in formatting
                 * @returns {*}
                 */
                $scope.toPrettyJSON = function (objStr, tabWidth) {
                    var obj = null;
                    try {
                        obj = $parse(objStr)({});
                    } catch (e) {
                        $log.error(e);
                        return _lastGoodResult;
                    }

                    var result = JSON.stringify(obj, null, Number(tabWidth));
                    _lastGoodResult = result;

                    return result;
                };

                $scope.updatePartition = function (partition) {
                    $scope.partition = partition;

                    // if the current offset is not set, set it at the starting offset.
                    ensureOffset(partition);

                    // load the first message
                    if ($scope.topic.totalMessages > 0 && $scope.partition.offset) {
                        $scope.loadMessage();
                    }
                    else {
                        $scope.clearMessage();
                    }
                };

                $scope.updateTopic = function (topic) {
                    $scope.topic = topic;

                    // asynchronously load the replicas for the topic
                    var myTopicName = topic ? topic.topic : null;
                    if(myTopicName) {
                        $scope.getReplicas(myTopicName);
                    }

                    var partitions = topic != null ? topic.partitions : null;
                    if (partitions) {
                        $scope.updatePartition(partitions[0]);
                    }
                    else {
                        console.log("No partitions found");
                        $scope.partition = {};
                        $scope.clearMessage();
                    }
                };

                function ensureOffset(partition) {
                    if(!partition.offset) {
                        partition.offset = partition.startOffset;
                    }
                }

                /**
                 * Attempts to find and return the first non-empty topic; however, if none are found, it returns the
                 * first topic in the array
                 * @param topicSummaries the given array of topic summaries
                 * @returns the first non-empty topic
                 */
                function findNonEmptyTopic(topicSummaries) {
                    for (var n = 0; n < topicSummaries.length; n++) {
                        var ts = topicSummaries[n];
                        if (ts.totalMessages > 0) return ts;
                    }
                    return topicSummaries.length > 0 ? topicSummaries[0] : null;
                }

                function findPartitionByID(topic, partitionId) {
                    var partitions = topic.partitions;
                    for (var n = 0; n < partitions.length; n++) {
                        if (partitions[n].partition == partitionId) return partitions[n];
                    }
                    return null;
                }

                function findTopicByName(topicId) {
                    var topics = $scope.topics;
                    for (var n = 0; n < topics.length; n++) {
                        if (topics[n].topic == topicId) return topics[n];
                    }
                    return null;
                }

                // initially, clear the message
                $scope.clearMessage();

                /**
                 * Watch for topic changes, and when one occurs:
                 * 1. select the first non-empty topic
                 * 2. load the replicas for the topic
                 */
                $scope.$watch("Topics.topics", function(newTopics, oldTopics) {
                    $log.info("DashboardCtrl: Loaded new topics (" + newTopics.length + ")");
                    $scope.topics = newTopics;

                    var myTopic = findNonEmptyTopic($scope.topics);
                    $scope.updateTopic(myTopic);
                });

            }]);

})();