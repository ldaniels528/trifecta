/**
 * Trifecta Dashboard Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('DashboardCtrl', ['$scope', '$interval', '$log', '$parse', '$timeout', 'DashboardSvc', 'DecoderMgmtSvc', 'MessageSearchSvc',
            function ($scope, $interval, $log, $parse, $timeout, DashboardSvc, DecoderMgmtSvc, MessageSearchSvc) {

                $scope.version = "0.18.1";
                $scope.consumerMapping = [];
                $scope.decoders = [];
                $scope.topics = [];
                $scope.hideEmptyTopics = false;
                $scope.realTimeViewMode = 'consumers';

                clearDecoder();
                clearMessage();

                $scope.tabs = [
                    {
                        "name": "Observe",
                        "imageURL": "/app/images/tabs/main/realTime.png",
                        "active": false
                    }, {
                        "name": "Inspect",
                        "imageURL": "/app/images/tabs/main/inspect.png",
                        "active": false
                    }, {
                        "name": "Query",
                        "imageURL": "/app/images/tabs/main/queries.png",
                        "active": false
                    }
                ];

                $scope.realTimeTabs = [
                    {
                        "name": "Topics",
                        "imageURL": "/app/images/tabs/realTime/topics.png",
                        "active": false
                    }, {
                        "name": "Consumers",
                        "imageURL": "/app/images/tabs/realTime/consumers.png",
                        "active": false
                    }
                ];

                // select the default tabs
                $scope.tab = $scope.tabs[1];
                $scope.tab.active = true;
                $scope.realTimeTab = $scope.realTimeTabs[0];
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

                $scope.changeRealTimeView = function (index, event) {
                    $scope.realTimeTab = $scope.realTimeTabs[index];

                    if (event) {
                        event.preventDefault();
                    }
                };

                $scope.manageDecoders = function () {
                    $scope.form = {
                        "topic": $scope.topic,
                        "decoderURL": ($scope.decoder.name ? $scope.decoder.name : null)
                    };
                    DecoderMgmtSvc.popup($scope,
                        function (response) {
                            $log.info("response = " + angular.toJson(response));
                        },
                        function (err) {
                            $log.error(err);
                        });
                };

                $scope.messageFinderPopup = function () {
                    MessageSearchSvc.finderDialog($scope).then(function (form) {
                        form.topic = form.topic.topic;
                        form.decoderURL = $scope.decoder.name ? $scope.decoder.name : null;
                        $log.info("form = " + angular.toJson(form));
                        // TODO validate the form

                        if (form.topic && form.decoderURL && form.criteria) {
                            // display the loading dialog
                            var loadingDialog = MessageSearchSvc.loadingDialog($scope);

                            // perform the search
                            DashboardSvc.findOne(form.topic, form.decoderURL, form.criteria)
                                .then(function (message) {
                                    $log.info("message = " + angular.toJson(message));
                                    $scope.message = message;

                                    loadingDialog.close({});

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
                                });
                        }
                    });
                };

                function findTopicByName(topicId) {
                    var topics = $scope.topics;
                    for (var n = 0; n < topics.length; n++) {
                        if (topics[n].topic == topicId) return topics[n];
                    }
                    return null;
                }

                function findPartitionByID(topic, partitionId) {
                    var partitions = topic.partitions;
                    for (var n = 0; n < partitions.length; n++) {
                        if (partitions[n].partition == partitionId) return partitions[n];
                    }
                    return null;
                }

                $scope.formatDecoder = function (decoder) {
                    if (decoder.name == "(None)") return decoder.name;
                    else {
                        var label = decoder.type;
                        if ("" != decoder.name) {
                            label += " - " + decoder.name;
                        }
                        return label;
                    }
                };

                $scope.getTopics = function (hideEmptyTopics) {
                    return $scope.topics.filter(function (topic) {
                        return !hideEmptyTopics || topic.totalMessages > 0;
                    });
                };

                $scope.loadMessage = function () {
                    clearMessage();
                    if ($scope.topic.totalMessages > 0) {
                        var topic = $scope.topic.topic;
                        var partition = $scope.partition.partition;
                        var offset = $scope.partition.offset;
                        var decoder = $scope.decoder.name;

                        DashboardSvc.getMessage(topic, partition, offset, decoder).then(
                            function (message) {
                                $scope.message = message;
                            },
                            function (err) {
                                $log.error(err);
                            });
                    }
                };

                $scope.firstMessage = function () {
                    if ($scope.partition.offset != $scope.partition.startOffset) {
                        $scope.partition.offset = $scope.partition.startOffset;
                        $scope.loadMessage();
                    }
                };

                $scope.lastMessage = function () {
                    if ($scope.partition.offset != $scope.partition.endOffset) {
                        $scope.partition.offset = $scope.partition.endOffset;
                        $scope.loadMessage();
                    }
                };

                $scope.nextMessage = function () {
                    var offset = $scope.partition.offset;
                    if (offset && offset < 1 + $scope.partition.endOffset) {
                        $scope.partition.offset += 1;
                        $scope.loadMessage();
                    }
                };

                $scope.previousMessage = function () {
                    var offset = $scope.partition.offset;
                    if (offset && offset > $scope.partition.startOffset) {
                        $scope.partition.offset -= 1;
                        $scope.loadMessage();
                    }
                };

                var _lastGoodResult = "";
                $scope.toPrettyJSON = function (objStr, tabWidth) {
                    try {
                        var obj = $parse(objStr)({});
                    } catch (e) {
                        // eat $parse error
                        return _lastGoodResult;
                    }

                    var result = JSON.stringify(obj, null, Number(tabWidth));
                    _lastGoodResult = result;

                    return result;
                };

                $scope.updateConsumers = function () {
                    DashboardSvc.getConsumers().then(
                        function (consumers) {
                            if ((consumers || []).length > 0) {
                                angular.forEach($scope.consumerMapping, function (root) {
                                    root.loading = true;

                                    angular.forEach(root.consumers, function (consumer) {
                                        angular.forEach(consumer.details, function (c) {
                                            var nc = findConsumer(consumers, c);
                                            if (nc && (c.offset != nc.offset || c.topicOffset != nc.topicOffset ||
                                                c.messagesLeft != nc.messagesLeft || c.lastModified != nc.lastModified)) {
                                                c.offset = nc.offset;
                                                c.topicOffset = nc.topicOffset;
                                                c.messagesLeft = nc.messagesLeft;
                                                c.lastModified = nc.lastModified;
                                            }
                                        });
                                    });
                                    $timeout(function () {
                                        root.loading = false;
                                    }, 500);
                                });
                            }
                            else {
                                console.log("No consumers found");
                            }
                        },
                        function (err) {
                            $log.error(err);
                        });
                };

                function findConsumer(consumers, consumer) {
                    for (var n = 0; n < consumers.length; n++) {
                        var c = consumers[n];
                        if (c.topic == consumer.topic && c.partition == consumer.partition && c.consumerId == consumer.consumerId) {
                            return c;
                        }
                    }
                    return null;
                }

                $scope.updatePartition = function (partition) {
                    $scope.partition = partition;

                    // if the current offset is not set, set it at the starting offset.
                    if (!$scope.partition.offset) {
                        $scope.partition.offset = $scope.partition.startOffset;
                    }

                    // load the first message
                    if ($scope.topic.totalMessages > 0 && $scope.partition.offset) {
                        $scope.loadMessage();
                    }
                    else {
                        clearMessage();
                    }
                };

                $scope.updateTopic = function (topic) {
                    $scope.topic = topic;

                    var partitions = topic != null ? topic.partitions : null;
                    if (partitions) {
                        $scope.updatePartition(partitions[0]);
                    }
                    else {
                        console.log("No partitions found");
                        $scope.partition = {};
                        clearMessage();
                    }
                };

                function clearDecoder() {
                    $scope.decoder = {};
                }

                function clearMessage() {
                    $scope.message = {};
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
                        if (ts.totalMessages > 0) {
                            return ts;
                        }
                    }
                    return topicSummaries.length > 0 ? topicSummaries[0] : null;
                }

                /**
                 * Initializes all reference data
                 */
                $scope.initReferenceData = function () {
                    // load the topics
                    DashboardSvc.getTopics().then(
                        function (topics) {
                            if (topics) {
                                $scope.topics = topics;
                                $scope.updateTopic(findNonEmptyTopic($scope.topics));
                            }
                            else {
                                console.log("No topic summaries found");
                                $scope.topics = [];
                                $scope.topic = {};
                                $scope.partition = {};
                            }
                        },
                        function (err) {
                            $log.error(err);
                        });

                    // load the consumer mapping
                    DashboardSvc.getConsumerMapping().then(
                        function (consumerSet) {
                            if (consumerSet) {
                                $scope.consumerMapping = consumerSet;
                            }
                            else {
                                console.log("No consumer mappings found");
                                $scope.consumerMapping = [];
                            }
                        },
                        function (err) {
                            $log.error(err);
                        });

                    // load the decoders
                    DashboardSvc.getDecoders().then(
                        function (decoders) {
                            if (decoders && decoders.length > 0) {
                                $scope.decoders = decoders;
                                $scope.decoder = $scope.decoders[0];
                            }
                            else {
                                console.log("No decoders found");
                                $scope.decoders = [];
                                clearDecoder();
                            }
                        },
                        function (err) {
                            $log.error(err);
                        });

                    // check for consumer updates every 15 seconds
                    $interval(function () {
                        $scope.updateConsumers();
                    }, 15000);
                };

            }]);

})();