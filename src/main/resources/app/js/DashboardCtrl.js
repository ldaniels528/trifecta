/**
 * Trifecta Dashboard Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');
    app.controller('DashboardCtrl', ['$scope', '$log', 'DashboardSvc', function ($scope, $log, DashboardSvc) {

        $scope.decoders = [
            {
                "type": "Avro",
                "name": "quotes.avsc"
            }, {
                "type": "Bytes",
                "name": ""
            }, {
                "type": "Json",
                "name": ""
            }, {
                "type": "Text",
                "name": ""
            }];

        $scope.decoder = $scope.decoders[0];

        $scope.topics = [];

        $scope.formatDecoder = function (decoder) {
            var label = decoder.type;
            if (decoder.name != "") label += " - " + decoder.name;
            return label
        };

        $scope.message = function () {
            return angular.toJson({
                "symbol": "KGMEF",
                "exchange": null,
                "lastTrade": 0.0,
                "tradeDate": null,
                "tradeTime": null,
                "ask": null,
                "bid": null,
                "change": null,
                "changePct": null,
                "prevClose": null,
                "open": null,
                "close": 0.0,
                "high": null,
                "low": null,
                "volume": null,
                "marketCap": null,
                "errorMessage": "Ticker symbol has changed to: <a href=/q?s=KGMEF>KGMEF</a>"
            }, true);
        };

        $scope.updatePartition = function (partition) {
            $scope.partition = partition;

            if (!$scope.partition.offset) {
                $scope.partition.offset = $scope.partition.startOffset;
            }
        };

        $scope.updateTopic = function (topic) {
            $scope.topic = topic;
            $scope.updatePartition(topic.partitions[0]);
            // TODO check for empty partitions?
        };

        // initialize the controller
        DashboardSvc.getTopics().then(
            function(topics) {
                $scope.loading = false;
                $scope.topics = topics;
                $scope.updateTopic($scope.topics[0]);
            },
            function(err) {
                $scope.loading = false;
                $log.error("ERROR: " + err)
            });
    }])

})();