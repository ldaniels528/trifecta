/**
 * Trifecta Application Controller
 * @author lawrence.daniels@gmail.com
 */
(function () {
    var app = angular.module('trifecta', []);

    app.controller('DashboardCtrl', ['$scope', '$log', function ($scope, $log) {

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

        $scope.topics = [
            {
                "topic": "com.shocktrade.quotes.csv",
                "partitions": [{
                    "partition": 0,
                    "startOffset": 32050,
                    "endOffset": 36543,
                    "messages": 4493,
                    "leader": "dev501:9092",
                    "replicas": [{
                        "broker": "dev501:9092", "status": "in-sync"
                    }, {
                        "broker": "dev501:9093", "status": "in-sync"
                    }, {
                        "broker": "dev502:9092", "status": "in-sync"
                    }, {
                        "broker": "dev502:9093", "status": "in-sync"
                    }]
                }, {
                    "partition": 1,
                    "startOffset": 33099,
                    "endOffset": 37720,
                    "messages": 4621
                }, {
                    "partition": 2,
                    "startOffset": 32291,
                    "endOffset": 36804,
                    "messages": 4513
                }, {
                    "partition": 3,
                    "startOffset": 33241,
                    "endOffset": 37846,
                    "messages": 4605
                }, {
                    "partition": 4,
                    "startOffset": 32094,
                    "endOffset": 36616,
                    "messages": 4522
                }]
            }, {
                "topic": "hft.shocktrade.quotes.avro",
                "partitions": [
                    {
                        "partition": 0,
                        "startOffset": 33099,
                        "endOffset": 37720,
                        "messages": 4621
                    }]
            }, {
                "topic": "shocktrade.quotes.csv",
                "partitions": [
                    {
                        "partition": 0,
                        "startOffset": 32050,
                        "endOffset": 36543,
                        "messages": 4493
                    }]
            }, {
                "topic": "shocktrade.quotes.avro",
                "partitions": [
                    {
                        "partition": 0,
                        "startOffset": 32050,
                        "endOffset": 36543,
                        "messages": 4493
                    }]
            }, {
                "topic": "shocktrade.keystats.avro",
                "partitions": [
                    {
                        "partition": 0,
                        "startOffset": 32050,
                        "endOffset": 36543,
                        "messages": 4493
                    }]
            }, {
                "topic": "test.Shocktrade.quotes.avro",
                "partitions": [
                    {
                        "partition": 0,
                        "startOffset": 32050,
                        "endOffset": 36543,
                        "messages": 4493
                    }]
            }, {
                "topic": "test1.Shocktrade.quotes.avro",
                "partitions": [
                    {
                        "partition": 0,
                        "startOffset": 32050,
                        "endOffset": 36543,
                        "messages": 4493
                    }]
            }, {
                "topic": "test2.Shocktrade.quotes.avro",
                "partitions": [
                    {
                        "partition": 0,
                        "startOffset": 32050,
                        "endOffset": 36543,
                        "messages": 4493
                    }]
            }, {
                "topic": "test3.Shocktrade.quotes.avro",
                "partitions": [
                    {
                        "partition": 0,
                        "startOffset": 32050,
                        "endOffset": 36543,
                        "messages": 4493
                    }]
            }];

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

            if (!partition.offset) {
                partition.offset = partition.startOffset;
            }
        };

        $scope.updateTopic = function (topic) {
            $scope.topic = topic;
            $scope.updatePartition(topic.partitions[0]);
            // TODO check for empty partitions?
        };

        // initialize the controller
        $scope.updateTopic($scope.topics[0]);
    }])

})();