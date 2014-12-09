/**
 * Decoder Management Dialog (Controller & Service)
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');

    /**
     * Decoder Management Dialog Service
     */
    app.factory('DecoderMgmtSvc', function ($http, $log, $modal) {
        var service = {};

        service.findOne = function (topic, decoderURL, criteria) {
            return $http.get("/rest/findOne/" + topic + "/" + decoderURL + "/" + encodeURI(criteria))
                .then(function (response) {
                    return response.data;
                });
        };

        /**
         * Decoder Management Modal Dialog
         */
        service.popup = function ($scope, successCB, errorCB) {

            // create an instance of the dialog
            var $modalInstance = $modal.open({
                controller: 'DecoderMgmtCtrl',
                templateUrl: 'decoder_mgmt.htm',
                resolve: {
                    form: function () {
                        return $scope.form;
                    }
                }
            });

            $modalInstance.result.then(function (form) {
                form.decoderURL = $scope.decoder.name ? $scope.decoder.name : null;
                if (form && form.topic && form.topic.topic) {
                    form.topic = form.topic.topic;
                }
                $log.info("form[B] = " + angular.toJson(form));
                service.findOne(form.topic, form.decoderURL, form.criteria).then(
                    function (response) {
                        if (successCB) successCB(response);
                    },
                    function (err) {
                        if (errorCB) errorCB(err);
                    }
                );
            }, function () {
                $log.info('Modal dismissed at: ' + new Date());
            });
        };

        return service;
    });

    /**
     * Decoder Management Dialog Controller
     */
    app.controller('DecoderMgmtCtrl', ['$scope', '$log', '$modalInstance', 'DashboardSvc',
        function ($scope, $log, $modalInstance, DashboardSvc) {

            $scope.form = null;
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