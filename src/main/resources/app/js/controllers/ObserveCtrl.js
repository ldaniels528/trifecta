/**
 * Observe Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta').controller('ObserveCtrl', ['$scope', '$log', '$timeout', '$interval', 'ConsumerSvc', 'ZookeeperSvc',
        function ($scope, $log, $timeout, $interval, ConsumerSvc, ZookeeperSvc) {

            $scope.consumerMapping = [];
            $scope.formats = ["auto", "binary", "json", "plain-text"];
            $scope.selected = { "format": $scope.formats[0] };
            $scope.zkItem = null;
            $scope.zkItems = [{ name: "/ (root)", path: "/", expanded: false }];

            $scope.showConcurrencyConfig = false;

            $scope.observeTabs = [
                {
                    "name": "Consumers",
                    "imageURL": "/app/images/tabs/observe/consumers.png",
                    "active": false
                }, {
                    "name": "Topics",
                    "imageURL": "/app/images/tabs/observe/topics.png",
                    "active": false
                }, {
                    "name": "Zookeeper",
                    "imageURL": "/app/images/tabs/observe/zookeeper.png",
                    "active": false
                }
            ];

            // select the default tab and make it active
            $scope.observeTab = $scope.observeTabs[0];
            $scope.observeTab.active = true;

            /**
             * Expands the first Zookeeper item
             */
            $scope.expandFirstItem = function () {
                // load the children for the root key
                if($scope.zkItems.length) {
                    var firstItem = $scope.zkItems[0];
                    if (firstItem) {
                        $scope.expandItem(firstItem);
                        $scope.getItemInfo(firstItem);
                    }
                }
            };

            /**
             * Expands or collapses the given Zookeeper item
             * @param item the given Zookeeper item
             */
            $scope.expandItem = function(item) {
                item.expanded = !item.expanded;
                if(item.expanded) {
                    item.loading = true;
                    ZookeeperSvc.getZkPath(item.path).then(
                        function (zkItems) {
                            item.loading = false;
                            item.children = zkItems;
                        },
                        function(err) {
                            item.loading = false;
                            errorHandler(err);
                        });
                }
            };

            $scope.formatData = function(path, format) {
                ZookeeperSvc.getZkData(path, format).then(
                    function (data) {
                        $scope.zkItem.data = data;
                        if(format == 'auto') {
                            $scope.selected.format = data.type;
                        }
                    },
                    function(err) {
                        errorHandler(err);
                    });
            };

            $scope.getItemInfo = function(item) {
                item.loading = true;
                ZookeeperSvc.getZkInfo(item.path).then(
                    function (itemInfo) {
                        item.loading = false;
                        //$scope.selected.format = $scope.formats[0];
                        $scope.zkItem = itemInfo;
                    },
                    function(err) {
                        item.loading = false;
                        errorHandler(err);
                    });
            };

            $scope.changeObserveTab = function (index, event) {
                $scope.observeTab = $scope.observeTabs[index];
                if (event) {
                    event.preventDefault();
                }
            };

            $scope.isConsumerUpToDate = function(consumer) {
                var details = consumer.details || [];
                if(!details.length) return false;
                else {
                    var time = new Date().getTime() - 300000; // 5 minutes ago
                    for(var n = 0; n < details.length; n++) {
                        if(details[n].lastModified >= time) return true;
                    }
                    return false;
                }
            };

            $scope.updateConsumers = function () {
                ConsumerSvc.getConsumers().then(
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
                    errorHandler);
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

            function errorHandler(err) {
                $scope.addError(err);
            }

            $scope.$watch("ConsumerSvc.consumers", function(newConsumers, oldConsumers) {
                if(newConsumers && newConsumers.length) {
                    $log.info("Loaded new consumers (" + newConsumers.length + ")");
                    $scope.consumerMapping = newConsumers;
                }
            });

            /*
            $scope.$watch("TopicSvc.topics", function(newTopics, oldTopics) {
                if(newTopics.length) {
                    $scope.expandItem(newTopics[0]);
                }
            });*/

        }])
})();