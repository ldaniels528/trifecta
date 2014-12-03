/**
 * Queries Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta').controller('ObserveCtrl', ['$scope', '$log', '$timeout', '$interval', 'DashboardSvc',
        function ($scope, $log, $timeout, $interval, DashboardSvc) {

            $scope.consumerMapping = [];
            $scope.formats = ["auto", "binary", "json", "plain-text"];
            $scope.selected = { "format": $scope.formats[0] };
            $scope.zkItem = null;
            $scope.zkItems = [{ name: "/ (root)", path: "/", expanded: false }];

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
             * Expands or collapses the given Zookeeper item
             * @param item the given Zookeeper item
             */
            $scope.expandItem = function(item) {
                item.expanded = !item.expanded;
                if(item.expanded) {
                    item.loading = true;
                    DashboardSvc.getZkPath(item.path).then(
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
                DashboardSvc.getZkData(path, format).then(
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
                DashboardSvc.getZkInfo(item.path).then(
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

            /**
             * Initializes all reference data
             */
            $scope.initReferenceData = function () {
                // load the children for the root key
                var firstItem = $scope.zkItems[0];
                $scope.expandItem(firstItem);
                $scope.getItemInfo(firstItem);
            };

            $scope.changeObserveTab = function (index, event) {
                $scope.observeTab = $scope.observeTabs[index];

                if (event) {
                    event.preventDefault();
                }
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
                $log.error(err);
            }

            $scope.$watch("Consumers.consumers", function(newVal, oldVal) {
                $log.info("ObserveCtrl: Loaded new consumers (" + newVal.length + ")");
                $scope.consumerMapping = newVal;
            });


        }])
})();