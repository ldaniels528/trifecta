/**
 * Trifecta Message Search Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');
    app.factory('MessageSearchSvc', function($http, $log, $modal) {
        var service = { };

        service.findOne = function(topic, decoderURL, criteria) {
            return $http.get("/rest/findOne/" + topic + "/" + decoderURL + "/" + encodeURI(criteria))
                .then(function(response) {
                    return response.data;
                });
        };

        /**
         * Message Search Modal Dialog
         */
        service.popup = function($scope, decoderURL, successCB, errorCB) {
            // create an instance of the dialog
            var $modalInstance = $modal.open({
                controller: 'MessageSearchCtrl',
                templateUrl: 'message_search.htm',
                resolve: {
                    form: function() {
                        return $scope.form;
                    }
                }
            });

            $modalInstance.result.then(function(form) {
                form.decorderURL = decoderURL;
                if(form && form.topic && form.topic.topic) {
                    form.topic = form.topic.topic;
                }
                $log.info("form[B] = " + angular.toJson(form));
                service.findOne(form.topic, "quotes.avsc", form.criteria).then(
                    function(response) {
                        if(successCB) successCB(response);
                    },
                    function(err) {
                        if(errorCB) errorCB(err);
                    }
                );
            }, function () {
                $log.info('Modal dismissed at: ' + new Date());
            });
        };

        return service;
    });

})();