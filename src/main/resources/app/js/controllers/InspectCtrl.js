/**
 * Inspect Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('InspectCtrl', ['$scope', '$interval', '$log', '$parse', '$timeout', 'MessageSvc', 'MessageSearchSvc', 'TopicSvc', 'WebSockets',
            function ($scope, $interval, $log, $parse, $timeout, MessageSvc, MessageSearchSvc, TopicSvc, WebSockets) {

                $scope.hideEmptyTopics = true;
                $scope.topics = TopicSvc.topics;
                $scope.topic = null;
                $scope.loading = 0;
                $scope.message = {};

                $scope.displayMode = {
                    "state" : "message",
                    "avro" : "json"
                };

                $scope.sampling = {
                    "status": "stopped"
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

                /**
                 * Retrieves message data for the given offset within the topic partition.
                 * @param topic the given topic
                 * @param partition the given partition
                 * @param offset the given offset
                 */
                $scope.getMessageData = function (topic, partition, offset) {
                    $scope.clearMessage();
                    $scope.loading++;

                    // ensure the loading animation stops
                    var promise = $timeout(function() {
                        $log.warn("Timeout reached for loading animation: loading = " + $scope.loading);
                        if($scope.loading) $scope.loading--;
                    }, 5000);

                    MessageSvc.getMessage(topic, partition, offset).then(
                        function (message) {
                            $scope.message = message;
                            if($scope.loading) $scope.loading--;
                            $timeout.cancel(promise);
                        },
                        function (err) {
                            $scope.addError(err);
                            if($scope.loading) $scope.loading--;
                            $timeout.cancel(promise);
                        });
                };

                $scope.setMessageData = function(message) {
                    $scope.message = message;

                    // update the partition with the offset
                    var partition = TopicSvc.findPartition($scope.topic, message.partition);
                    if(partition) {
                        $scope.partition = partition;
                        partition.offset = message.offset;
                    }
                };

                /**
                 * Retrieves message key for the given offset within the topic partition.
                 * @param topic the given topic
                 * @param partition the given partition
                 * @param offset the given offset
                 */
                $scope.getMessageKey = function (topic, partition, offset) {
                    $scope.clearMessage();
                    $scope.loading++;

                    // ensure the loading animation stops
                    var promise = $timeout(function() {
                        $log.warn("Timeout reached for loading animation: loading = " + $scope.loading);
                        if($scope.loading) $scope.loading--;
                    }, 5000);

                    MessageSvc.getMessageKey(topic, partition, offset).then(
                        function (message) {
                            $scope.message = message;
                            if($scope.loading) $scope.loading--;
                            $timeout.cancel(promise);
                        },
                        function (err) {
                            $scope.addError(err);
                            if($scope.loading) $scope.loading--;
                            $timeout.cancel(promise);
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

                $scope.getTopicIcon = function(topic, selected) {
                    if(!topic.totalMessages) return "/app/images/common/topic_alert-16.png";
                    else if(selected) return "/app/images/common/topic_selected-16.png";
                    else return "/app/images/common/topic-16.png";
                };

                $scope.getTopicIconSelection = function(selected) {
                    return selected
                        ? "/app/images/common/topic_selected-16.png"
                        : "/app/images/common/topic-16.png";
                };

                $scope.getTopics = function (hideEmptyTopics) {
                    return $scope.topics.filter(function (topic) {
                        return !hideEmptyTopics || topic.totalMessages > 0;
                    });
                };

                $scope.getTopicNames = function (hideEmptyTopics) {
                    var topics = $scope.getTopics(hideEmptyTopics);
                    return topics.map(function(t) {
                        return t.topic;
                    });
                };

                $scope.gotoDecoder = function(topic) {
                    if(angular.element('#Decoders').scope().switchToDecoderByTopic(topic)) {
                        $scope.changeTab(4, null); // Decoders
                    }
                };

                $scope.isLimitedControls = function() {
                    return $scope.sampling.status == 'started';
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
                            $log.error("Unrecognized display mode (mode = " + $scope.displayMode.state + ")");
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

                $scope.messageSamplingStart = function(topic) {
                    // build the request
                    var partitions = [];
                    angular.forEach(topic.partitions, function(p) {
                        partitions.push(p.offset != null ? p.offset : p.endOffset);
                    });
                    var json = angular.toJson({
                        "action": "startMessageSampling",
                        "topic": topic.topic,
                        "partitions": partitions
                    });

                    // transfer the request
                    if(WebSockets.send(json)) {
                        $scope.sampling.status = "started";
                    }
                    else {
                        $scope.addErrorMessage("Failed to start message sampling");
                    }
                };

                $scope.messageSamplingStop = function(topic) {
                    // build the request
                    var json = angular.toJson({ "action": "stopMessageSampling", "topic": topic.topic });

                    // transfer the request
                    if(WebSockets.send(json)) {
                        $scope.sampling.status = "stopped";
                    }
                    else {
                        $scope.addErrorMessage("Failed to stop message sampling");
                    }
                };

                $scope.nextMessage = function () {
                    ensureOffset($scope.partition);
                    var offset = $scope.partition.offset;
                    if (offset < $scope.partition.endOffset) {
                        $scope.partition.offset += 1;
                    }
                    $scope.loadMessage();
                };

                $scope.previousMessage = function () {
                    ensureOffset($scope.partition);
                    var offset = $scope.partition.offset;
                    if (offset && offset > $scope.partition.startOffset) {
                        $scope.partition.offset -= 1;
                    }
                    $scope.loadMessage();
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
                        $scope.changeTab(0, null); // Inspect
                    }
                };

                /**
                 * Toggles the Avro/JSON output flag
                 */
                $scope.toggleAvroOutput = function() {
                    $scope.displayMode.avro = $scope.displayMode.avro == 'json' ? 'avro' : 'json';
                };

                /**
                 * Toggles the empty topic hide/show flag
                 */
                $scope.toggleHideShowEmptyTopics = function() {
                    $scope.hideEmptyTopics = !$scope.hideEmptyTopics;
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
                        //$scope.addErrorMessage("Error parsing JSON document");
                        $log.error(e);
                        return "";
                    }
                    return JSON.stringify(obj, null, Number(tabWidth));
                };

                $scope.updatePartition = function (partition) {
                    $scope.partition = partition;

                    // if the current offset is not set, set it at the starting offset.
                    ensureOffset(partition);

                    // load the first message
                    $scope.loadMessage();
                };

                $scope.updateTopic = function (topic) {
                    $scope.topic = topic;

                    var partitions = topic ? topic.partitions : null;
                    if (partitions) {
                        var partition = partitions[0];
                        $scope.updatePartition(partition);

                        $log.info("topic = " + ( $scope.topic ? $scope.topic.topic : null ) +
                        ", partition = " + ($scope.partition ? $scope.partition.partition : null ) +
                        ", offset = " + ($scope.partition ? $scope.partition.offset : null ));

                        // load the message
                        //$scope.loadMessage();
                    }
                    else {
                        console.log("No partitions found");
                        $scope.partition = {};
                        $scope.clearMessage();
                    }
                };

                function ensureOffset(partition) {
                    if(partition && partition.offset == null) {
                        partition.offset = partition.endOffset;
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

                /**
                 * Watch for topic changes, and select the first non-empty topic
                 */
                $scope.$watchCollection("TopicSvc.topics", function(newTopics, oldTopics) {
                    $log.info("Loaded new topics (" + newTopics.length + ")");
                    //$scope.topics = newTopics;

                    if(!$scope.topic) {
                        var myTopic = findNonEmptyTopic($scope.topics);
                        $scope.updateTopic(myTopic);
                    }
                });

            }]);
})();