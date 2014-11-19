/**
 * Trifecta Message Search Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');
    app.controller('MessageSearchCtrl', ['$scope', '$log', '$modalInstance', 'DashboardSvc',
        function ($scope, $log, $modalInstance, DashboardSvc) {

            $scope.form = null;
            $scope.topics = [];

            $scope.getTopics = function(hideEmptyTopics) {
                return $scope.topics.filter(function(topic) {
                    return !hideEmptyTopics || topic.totalMessages > 0;
                });
            };

            $scope.ok = function() {
                $modalInstance.close($scope.form);
            };

            $scope.cancel = function() {
                $modalInstance.dismiss('cancel');
            };

            // load the topics
            DashboardSvc.getTopics().then(
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