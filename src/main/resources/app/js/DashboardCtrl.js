/**
 * Trifecta Dashboard Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');
    app.controller('DashboardCtrl', ['$scope', '$interval', '$log', '$parse', '$timeout', 'DashboardSvc', 'MessageSearchSvc',
        function ($scope, $interval, $log, $parse, $timeout, DashboardSvc, MessageSearchSvc) {

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
                    "name": "Real-time",
                    "imageURL": "/images/tabs/main/realTime.png",
                    "active": false
                }, {
                    "name": "Inspect",
                    "imageURL": "/images/tabs/main/inspect.png",
                    "active": false
                }, {
                    "name": "Decoders",
                    "imageURL": "/images/tabs/main/filters.png",
                    "active": false
                }, {
                    "name": "Queries",
                    "imageURL": "/images/tabs/main/queries.png",
                    "active": false
                }, {
                    "name": "Export",
                    "imageURL": "/images/tabs/main/export.png",
                    "active": false
                }
            ];

            $scope.realTimeTabs = [
                {
                    "name": "Topics",
                    "imageURL": "/images/tabs/realTime/topics.png",
                    "active": false
                }, {
                    "name": "Consumers",
                    "imageURL": "/images/tabs/realTime/consumers.png",
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
            $scope.changeTab = function(index, event) {
                // deactivate the current tab
                $scope.tab.active = false;

                // activate the new tab
                $scope.tab = $scope.tabs[index];
                $scope.tab.active = true;

                if(event) {
                    event.preventDefault();
                }
            };

            $scope.changeRealTimeView = function(index, event) {
                $scope.realTimeTab = $scope.realTimeTabs[index];

                if(event) {
                    event.preventDefault();
                }
            };

            $scope.findMessage = function() {
                var decoder = $scope.decoder;
                MessageSearchSvc.popup($scope, decoder.name ? decoder.name : null,
                    function(response) {
                        $log.info("response = " + angular.toJson(response));
                    },
                    function(err) {
                        $log.error(err);
                    });
            };

            $scope.formatDecoder = function (decoder) {
                if(decoder.name == "(None)") return decoder.name;
                else {
                    var label = decoder.type;
                    if ("" != decoder.name) {
                        label += " - " + decoder.name;
                    }
                    return label;
                }
            };

            $scope.getTopics = function(hideEmptyTopics) {
                return $scope.topics.filter(function(topic) {
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
                } catch(e) {
                    // eat $parse error
                    return _lastGoodResult;
                }

                var result = JSON.stringify(obj, null, Number(tabWidth));
                _lastGoodResult = result;

                return result;
            };

            $scope.updateConsumers = function() {
                DashboardSvc.getConsumers().then(
                    function (consumers) {
                        if (consumers && consumers.length > 0) {
                            angular.forEach($scope.consumerMapping, function(root) {
                                root.loading = true;

                                angular.forEach(root.consumers, function(consumer) {
                                    angular.forEach(consumer.details, function(c) {
                                        var nc = findConsumer(consumers, c);
                                        if(nc && (c.offset != nc.offset || c.topicOffset != nc.topicOffset ||
                                            c.messagesLeft != nc.messagesLeft || c.lastModified != nc.lastModified)) {
                                            c.offset = nc.offset;
                                            c.topicOffset = nc.topicOffset;
                                            c.messagesLeft = nc.messagesLeft;
                                            c.lastModified = nc.lastModified;
                                        }
                                    });
                                });
                                $timeout(function() { root.loading = false; }, 500);
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
                for(var n = 0; n < consumers.length; n++) {
                    var c = consumers[n];
                    if(c.topic == consumer.topic && c.partition == consumer.partition && c.consumerId == consumer.consumerId) {
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
                for(var n = 0; n < topicSummaries.length; n++) {
                    var ts = topicSummaries[n];
                    if(ts.totalMessages > 0) {
                        return ts;
                    }
                }
                return topicSummaries.length > 0 ? topicSummaries[0] : null;
            }

            // load the consumer mapping
            DashboardSvc.getConsumerMapping().then(
                function (consumerMapping) {
                    if (consumerMapping) {
                        $scope.consumerMapping = consumerMapping;
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


                // check for consumer updates every 15 seconds
                $interval(function() { $scope.updateConsumers(); }, 15000);
    }]);

})();