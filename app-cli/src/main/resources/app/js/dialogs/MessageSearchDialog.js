/**
 * Message Search Dialog (Controller & Service)
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');

    /**
     * Message Search Dialog Service
     */
    app.factory('MessageSearchSvc', function ($modal) {
        var service = {};

        /**
         * Message Search Finder Modal Dialog
         */
        service.finderDialog = function ($scope) {
            var $modalInstance = $modal.open({
                controller: 'MessageSearchCtrl',
                templateUrl: 'message_search_finder.htm',
                resolve: {
                    form: function () {
                        return $scope.form;
                    }
                }
            });
            return $modalInstance.result;
        };

        /**
         * Message Search Loading Modal Dialog
         */
        service.loadingDialog = function ($scope) {
            return $modal.open({
                controller: 'MessageSearchCtrl',
                templateUrl: 'message_search_loading.htm',
                resolve: {
                    form: function () {
                        return $scope.form;
                    }
                }
            });
        };

        return service;
    });

    /**
     * Message Search Dialog Controller
     */
    app.controller('MessageSearchCtrl', ['$scope', '$modalInstance', 'TopicSvc',
        function ($scope, $modalInstance, TopicSvc) {
            $scope.topics = [];

            $scope.getTopics = function (hideEmptyTopics) {
                return $scope.topics.filter(function (topic) {
                    return !hideEmptyTopics || topic.totalMessages > 0;
                });
            };

            $scope.ok = function () {
                $modalInstance.close($scope.form);
            };

            $scope.cancel = function () {
                $modalInstance.dismiss('cancel');
            };

            // load the topics
            TopicSvc.getTopics().then(
                function (topics) {
                    if (topics) {
                        $scope.topics = topics;
                    }
                    else {
                        console.log("No topic summaries found");
                        $scope.topics = [];
                    }
                },
                function (err) {
                    $log.error(err);
                });

        }]);
})();